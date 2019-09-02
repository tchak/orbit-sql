import { Record as OrbitRecord, RecordIdentity } from '@orbit/data';
import {
  RecordRelationshipIdentity,
  AsyncRecordCache,
  AsyncRecordCacheSettings
} from '@orbit/record-cache';
import Knex from 'knex';

import { ModelRegistry, buildModels, BaseModel } from './build-models';
import { migrateModels } from './migrate-models';
import { fieldsForType } from './utils';

export interface SQLCacheSettings extends AsyncRecordCacheSettings {
  knex: Knex.Config;
  autoMigrate?: boolean;
}

/**
 * A cache used to access records in an SQL database.
 *
 * Because SQL access is async, this cache extends `AsyncRecordCache`.
 */
export default class SQLCache extends AsyncRecordCache {
  protected _config: Knex.Config;
  protected _db: Knex;
  protected _models: ModelRegistry;

  autoMigrate: boolean;

  constructor(settings: SQLCacheSettings) {
    super(settings);

    this.autoMigrate = settings.autoMigrate !== false;
    this._config = settings.knex;
    this._models = buildModels(this.schema);
  }

  get config(): Knex.Config {
    return this._config;
  }

  async upgrade(): Promise<void> {
    await this.reopenDB();
    for (let processor of this._processors) {
      await processor.upgrade();
    }
  }

  async reset(): Promise<void> {
    await this.deleteDB();

    for (let processor of this._processors) {
      await processor.reset();
    }
  }

  get isDBOpen(): boolean {
    return !!this._db;
  }

  async openDB(): Promise<any> {
    if (!this.isDBOpen) {
      const db = Knex(this._config);
      if (this.autoMigrate) {
        await this.createDB(db);
      }
      this.connectDB(db);
      this._db = db;
    }
    return this._db;
  }

  connectDB(db: Knex) {
    for (let type of Object.keys(this._models)) {
      this._models[type] = this._models[type].bindKnex(db);
    }
  }

  async closeDB(): Promise<void> {
    if (this.isDBOpen) {
      await this._db.destroy();
    }
  }

  async reopenDB(): Promise<Knex> {
    await this.closeDB();
    return this.openDB();
  }

  async createDB(db: Knex): Promise<void> {
    await migrateModels(db, this.schema);
  }

  async deleteDB(): Promise<void> {
    await this.closeDB();
  }

  async clearRecords(type: string): Promise<void> {
    await this.queryBuilderForType(type).del();
  }

  async getRecordAsync(identity: RecordIdentity) {
    const fields = fieldsForType(this.schema, identity.type);
    const record = await this.queryBuilderForType(identity.type)
      .findById(identity.id)
      .select(fields);
    if (record) {
      return record.toOrbitRecord();
    }
    return;
  }

  async getRecordsAsync(
    typeOrIdentities?: string | RecordIdentity[]
  ): Promise<OrbitRecord[]> {
    let records: OrbitRecord[] = [];

    if (!typeOrIdentities) {
      for (let type in this.schema.models) {
        records = records.concat(await this.getRecordsAsync(type));
      }
    } else if (typeof typeOrIdentities === 'string') {
      let fields = fieldsForType(this.schema, typeOrIdentities);
      let models = await this.queryBuilderForType(typeOrIdentities).select(
        fields
      );
      records = models.map(model => model.toOrbitRecord());
    } else if (Array.isArray(typeOrIdentities)) {
      const identities: RecordIdentity[] = typeOrIdentities;

      if (identities.length > 0) {
        const idsByType = groupIdentitiesByType(identities);
        const recordsById: Record<string, OrbitRecord> = {};

        for (let type in idsByType) {
          let fields = fieldsForType(this.schema, type);
          for (let record of await this.queryBuilderForType(type)
            .findByIds(idsByType[type])
            .select(fields)) {
            recordsById[record.id] = record.toOrbitRecord();
          }
        }
        for (let identity of identities) {
          let record = recordsById[identity.id];
          if (record) {
            records.push(record);
          }
        }
      }
    }
    return records;
  }

  async setRecordAsync(record: OrbitRecord): Promise<void> {
    const properties = this.toProperties(record);

    await this.queryBuilderForType(record.type).upsertGraph(properties, {
      insertMissing: true,
      relate: true,
      unrelate: true
    });
  }

  async setRecordsAsync(records: OrbitRecord[]): Promise<void> {
    for (let record of records) {
      await this.setRecordAsync(record);
    }
  }

  async removeRecordAsync(identity: RecordIdentity) {
    const [record] = await this.removeRecordsAsync([identity]);
    return record;
  }

  async removeRecordsAsync(identities: RecordIdentity[]) {
    const records = await this.getRecordsAsync(identities);
    const idsByType = groupIdentitiesByType(records);
    for (let type in idsByType) {
      await this.queryBuilderForType(type).deleteById(idsByType[type]);
    }
    return records;
  }

  async getRelatedRecordAsync(
    identity: RecordIdentity,
    relationship: string
  ): Promise<RecordIdentity | null> {
    if (this.schema.hasRelationship(identity.type, relationship)) {
      const parent = await this.queryBuilderForType(identity.type).findById(
        identity.id
      );

      if (parent) {
        const { model: type } = this.schema.getRelationship(
          identity.type,
          relationship
        );
        const query = parent
          .$relatedQuery<BaseModel, BaseModel[]>(relationship)
          .select(fieldsForType(this.schema, type as string));
        const model = ((await query) as any) as (BaseModel | undefined);
        if (model) {
          return model.toOrbitRecord();
        }
      }
    }
    return null;
  }

  async getRelatedRecordsAsync(
    identity: RecordIdentity,
    relationship: string
  ): Promise<RecordIdentity[]> {
    if (this.schema.hasRelationship(identity.type, relationship)) {
      const parent = await this.queryBuilderForType(identity.type).findById(
        identity.id
      );

      if (parent) {
        const { model: type } = this.schema.getRelationship(
          identity.type,
          relationship
        );
        const query = parent
          .$relatedQuery<BaseModel, BaseModel[]>(relationship)
          .select(fieldsForType(this.schema, type as string));
        const models = await query;
        return models.map(model => model.toOrbitRecord());
      }
    }
    return [];
  }

  async getInverseRelationshipsAsync(
    recordIdentity: RecordIdentity
  ): Promise<RecordRelationshipIdentity[]> {
    recordIdentity;
    const recordRelationshipIdentities: RecordRelationshipIdentity[] = [];
    return recordRelationshipIdentities;
  }

  addInverseRelationshipsAsync(
    relationships: RecordRelationshipIdentity[]
  ): Promise<void> {
    if (relationships.length > 0) {
      return Promise.resolve();
    } else {
      return Promise.resolve();
    }
  }

  removeInverseRelationshipsAsync(
    relationships: RecordRelationshipIdentity[]
  ): Promise<void> {
    if (relationships.length > 0) {
      return Promise.resolve();
    } else {
      return Promise.resolve();
    }
  }

  protected queryBuilderForType(type: string) {
    return this._models[type].query();
  }

  protected toProperties(record: OrbitRecord) {
    const properties: Record<string, unknown> = {
      id: record.id
    };

    if (record.attributes) {
      this.schema.eachAttribute(record.type, property => {
        if (record.attributes && record.attributes[property] !== undefined) {
          properties[property] = record.attributes[property];
        }
      });
    }

    if (record.relationships) {
      this.schema.eachRelationship(record.type, (property, { type: kind }) => {
        if (record.relationships && record.relationships[property]) {
          if (kind === 'hasOne') {
            const data = record.relationships[property]
              .data as RecordIdentity | null;
            properties[property] = data ? { id: data.id } : null;
          } else {
            const data = record.relationships[property]
              .data as RecordIdentity[];
            properties[property] = data.map(({ id }) => ({ id }));
          }
        }
      });
    }

    return properties;
  }
}

function groupIdentitiesByType(identities: RecordIdentity[]) {
  const idsByType: Record<string, string[]> = {};
  for (let identity of identities) {
    idsByType[identity.type] = idsByType[identity.type] || [];
    idsByType[identity.type].push(identity.id);
  }
  return idsByType;
}
