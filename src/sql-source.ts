import Orbit, {
  Query,
  QueryOrExpression,
  queryable,
  Queryable,
  Source,
  SourceSettings,
  Transform,
  TransformOrOperations,
  RecordOperation,
  updatable,
  Updatable,
  Schema
} from '@orbit/data';
import Knex from 'knex';

import { Processor, ProcessorSettings } from './processor';

const { assert } = Orbit;

export interface SQLSourceSettings extends SourceSettings {
  knex?: Knex.Config;
  autoMigrate?: boolean;
}

/**
 * Source for storing data in SQL database.
 */
@queryable
@updatable
export default class SQLSource extends Source implements Queryable, Updatable {
  protected _processor: Processor;

  // Queryable interface stubs
  query: (
    queryOrExpression: QueryOrExpression,
    options?: object,
    id?: string
  ) => Promise<any>;

  // Updatable interface stubs
  update: (
    transformOrOperations: TransformOrOperations,
    options?: object,
    id?: string
  ) => Promise<any>;

  constructor(settings: SQLSourceSettings = {}) {
    assert(
      "SQLSource's `schema` must be specified in `settings.schema` constructor argument",
      !!settings.schema
    );

    assert(
      "SQLSource's `knex` must be specified in `settings.knex` constructor argument",
      !!settings.knex
    );

    settings.name = settings.name || 'sql';
    const autoActivate = settings.autoActivate;
    settings.autoActivate = false;

    super(settings);

    let processorSettings: ProcessorSettings = {
      knex: settings.knex as Knex.Config,
      schema: settings.schema as Schema,
      autoMigrate: settings.autoMigrate
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

  async _update(transform: Transform): Promise<any> {
    if (!this.transformLog.contains(transform.id)) {
      const results = await this._processor.patch(
        transform.operations as RecordOperation[]
      );
      await this.transformed([transform]);
      return transform.operations.length === 1 ? results[0] : results;
    }
  }

  /////////////////////////////////////////////////////////////////////////////
  // Queryable interface implementation
  /////////////////////////////////////////////////////////////////////////////

  async _query(query: Query): Promise<any> {
    return this._processor.query(query);
  }
}
