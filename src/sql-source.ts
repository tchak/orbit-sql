import Orbit, {
  Resettable,
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

import SQLCache, { SQLCacheSettings } from './sql-cache';
import Processor from './processor';

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
export default class SQLSource extends Source
  implements Resettable, Queryable, Updatable {
  protected _cache: SQLCache;
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

    let cacheSettings: SQLCacheSettings = {
      knex: settings.knex as Knex.Config,
      schema: settings.schema as Schema,
      autoMigrate: settings.autoMigrate
    };
    cacheSettings.keyMap = settings.keyMap;
    cacheSettings.queryBuilder =
      cacheSettings.queryBuilder || this.queryBuilder;
    cacheSettings.transformBuilder =
      cacheSettings.transformBuilder || this.transformBuilder;
    cacheSettings.knex = cacheSettings.knex || settings.knex;

    this._cache = new SQLCache(cacheSettings);
    this._processor = new Processor(this);

    if (autoActivate !== false) {
      this.activate();
    }
  }

  get cache(): SQLCache {
    return this._cache;
  }

  async _activate() {
    await super._activate();
    await this.cache.openDB();
  }

  async deactivate() {
    await super.deactivate();
    return this.cache.closeDB();
  }

  async upgrade(): Promise<void> {
    await this._cache.reopenDB();
  }

  /////////////////////////////////////////////////////////////////////////////
  // Resettable interface implementation
  /////////////////////////////////////////////////////////////////////////////

  async reset(): Promise<void> {
    await this._cache.reset();
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
    return this._cache.query(query);
  }
}
