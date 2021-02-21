import { Assertion } from '@orbit/core';
import { queryable, updatable, RequestOptions } from '@orbit/data';
import {
  RecordSourceQueryOptions,
  RecordSchema,
  RecordOperation,
  RecordSourceSettings,
  RecordQueryable,
  RecordUpdatable,
  RecordSource,
  RecordTransform,
  RecordQuery,
} from '@orbit/records';
import Knex from 'knex';

import { Processor, ProcessorSettings } from './processor';

export interface SQLQueryOptions extends RecordSourceQueryOptions {}

export interface SQLTransformOptions extends RequestOptions {}

export interface SQLSourceSettings
  extends RecordSourceSettings<SQLQueryOptions, SQLTransformOptions> {
  knex?: Knex.Config;
  autoMigrate?: boolean;
}

export interface SQLSource
  extends RecordSource<SQLQueryOptions, SQLTransformOptions>,
    RecordQueryable<unknown>,
    RecordUpdatable<unknown> {}

/**
 * Source for storing data in SQL database.
 */
@queryable
@updatable
export class SQLSource extends RecordSource<
  SQLQueryOptions,
  SQLTransformOptions
> {
  protected _processor: Processor;

  constructor(settings: SQLSourceSettings) {
    settings.name = settings.name || 'sql';

    if (!settings.schema) {
      new Assertion(
        "SQLSource's `schema` must be specified in `settings.schema` constructor argument"
      );
    }

    if (!settings.knex) {
      new Assertion(
        "SQLSource's `knex` must be specified in `settings.knex` constructor argument"
      );
    }

    const autoActivate = settings.autoActivate;
    settings.autoActivate = false;

    super(settings);

    let processorSettings: ProcessorSettings = {
      knex: settings.knex as Knex.Config,
      schema: settings.schema as RecordSchema,
      autoMigrate: settings.autoMigrate,
    };

    this._processor = new Processor(processorSettings);

    if (autoActivate !== false) {
      this.activate();
    }
  }

  async _activate() {
    await super._activate();
    await this._processor.openDB();
  }

  async deactivate() {
    await super.deactivate();
    return this._processor.closeDB();
  }

  /////////////////////////////////////////////////////////////////////////////
  // Updatable interface implementation
  /////////////////////////////////////////////////////////////////////////////

  async _update(transform: RecordTransform): Promise<any> {
    if (!this.transformLog.contains(transform.id)) {
      const data = await this._processor.patch(
        transform.operations as RecordOperation[]
      );
      await this.transformed([transform]);
      return {
        transform: [transform],
        data: transform.operations.length === 1 ? data[0] : data,
      };
    }
  }

  /////////////////////////////////////////////////////////////////////////////
  // Queryable interface implementation
  /////////////////////////////////////////////////////////////////////////////

  async _query(query: RecordQuery): Promise<any> {
    const data = await this._processor.query(query);
    return {
      transform: [],
      data: query.expressions.length === 1 ? data[0] : data,
    };
  }
}
