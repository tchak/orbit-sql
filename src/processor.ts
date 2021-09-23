import {
  Record as OrbitRecord,
  AddRecordOperation,
  UpdateRecordOperation,
  RemoveRecordOperation,
  ReplaceAttributeOperation,
  RemoveFromRelatedRecordsOperation,
  AddToRelatedRecordsOperation,
  ReplaceRelatedRecordsOperation,
  ReplaceRelatedRecordOperation,
  RecordOperation,
  FindRecord,
  FindRelatedRecords,
  FindRecords,
  FindRelatedRecord,
  RecordQuery,
  AttributeSortSpecifier,
  OffsetLimitPageSpecifier,
  RecordIdentity,
  AttributeFilterSpecifier,
  RecordSchema,
  RecordQueryExpression,
} from '@orbit/records';
import { QueryExpressionParseError } from '@orbit/data';
import Knex, { Config } from 'knex';
import { QueryBuilder, ModelClass, transaction, Transaction } from 'objection';
import { tableize, underscore, foreignKey } from 'inflected';

import { BaseModel, buildModels } from './build-models';
import { migrateModels } from './migrate-models';
import { groupRecordsByType } from './utils';

export interface ProcessorSettings {
  schema: RecordSchema;
  knex: Config;
  autoMigrate?: boolean;
}

export class Processor {
  schema: RecordSchema;
  autoMigrate: boolean;

  protected _config: Config;
  protected _db?: Knex;
  protected _models: Record<string, ModelClass<BaseModel>>;

  constructor(settings: ProcessorSettings) {
    this.schema = settings.schema;
    this.autoMigrate = settings.autoMigrate !== false;

    this._config = settings.knex;
    this._models = buildModels(this.schema);
  }

  async openDB(): Promise<any> {
    if (!this._db) {
      const db = Knex(this._config);
      if (this.autoMigrate) {
        await migrateModels(db, this.schema);
      }
      this._db = db;
    }
    return this._db;
  }

  async closeDB(): Promise<void> {
    if (this._db) {
      await this._db.destroy();
    }
  }

  async patch(operations: RecordOperation[]) {
    return transaction(this._db as Knex, async (trx) => {
      const data: OrbitRecord[] = [];
      for (let operation of operations) {
        data.push(await this.processOperation(operation, trx));
      }
      return data;
    });
  }

  async query(query: RecordQuery) {
    return transaction(this._db as Knex, async (trx) => {
      const data: (OrbitRecord | OrbitRecord[] | null)[] = [];
      const expressions = Array.isArray(query.expressions)
        ? query.expressions
        : [query.expressions];

      for (const expression of expressions) {
        data.push(await this.processQueryExpression(expression, trx));
      }
      return data;
    });
  }

  protected processOperation(operation: RecordOperation, trx: Transaction) {
    switch (operation.op) {
      case 'addRecord':
        return this.addRecord(operation, trx);
      case 'updateRecord':
        return this.updateRecord(operation, trx);
      case 'removeRecord':
        return this.removeRecord(operation, trx);
      case 'replaceAttribute':
        return this.replaceAttribute(operation, trx);
      case 'replaceRelatedRecord':
        return this.replaceRelatedRecord(operation, trx);
      case 'replaceRelatedRecords':
        return this.replaceRelatedRecords(operation, trx);
      case 'addToRelatedRecords':
        return this.addToRelatedRecords(operation, trx);
      case 'removeFromRelatedRecords':
        return this.removeFromRelatedRecords(operation, trx);
      default:
        throw new Error(`Unknown operation ${operation.op}`);
    }
  }

  protected processQueryExpression(
    expression: RecordQueryExpression,
    trx: Transaction
  ) {
    switch (expression.op) {
      case 'findRecord':
        return this.findRecord(expression as FindRecord, trx);
      case 'findRecords':
        return this.findRecords(expression as FindRecords, trx);
      case 'findRelatedRecord':
        return this.findRelatedRecord(expression as FindRelatedRecord, trx);
      case 'findRelatedRecords':
        return this.findRelatedRecords(expression as FindRelatedRecords, trx);
      default:
        throw new Error(`Unknown query ${expression}`);
    }
  }

  protected async addRecord(op: AddRecordOperation, trx: Transaction) {
    const qb = this.queryForType(trx, op.record.type);
    const data = this.parseOrbitRecord(op.record);

    const model = await qb.upsertGraph(data, {
      insertMissing: true,
      relate: true,
      unrelate: true,
    });

    return model.toOrbitRecord();
  }

  protected async updateRecord(op: UpdateRecordOperation, trx: Transaction) {
    const qb = this.queryForType(trx, op.record.type).context({
      recordId: op.record.id,
    });
    const data = this.parseOrbitRecord(op.record);

    const model = await qb.upsertGraph(data, {
      relate: true,
      unrelate: true,
    });

    return model.toOrbitRecord();
  }

  protected async removeRecord(op: RemoveRecordOperation, trx: Transaction) {
    const { type, id } = op.record;
    const qb = this.queryForType(trx, type).context({
      recordId: id,
    });

    const model = (await qb.findById(id)) as BaseModel;
    await qb.deleteById(id);

    return model.toOrbitRecord();
  }

  protected async replaceAttribute(
    op: ReplaceAttributeOperation,
    trx: Transaction
  ) {
    const { type, id } = op.record;
    const qb = this.queryForType(trx, type).context({
      recordId: id,
    });

    const model = await qb.patchAndFetchById(id, {
      [op.attribute]: op.value,
    });

    return model.toOrbitRecord();
  }

  protected async replaceRelatedRecord(
    op: ReplaceRelatedRecordOperation,
    trx: Transaction
  ) {
    const { type, id } = op.record;
    const qb = this.queryForType(trx, type).context({
      recordId: id,
    });
    const relatedId = op.relatedRecord ? op.relatedRecord.id : null;

    const model = (await qb.findById(id)) as BaseModel;
    if (relatedId) {
      await model.$relatedQuery(op.relationship, trx).relate(relatedId);
    } else {
      await model.$relatedQuery(op.relationship, trx).unrelate();
    }

    return model.toOrbitRecord();
  }

  protected async replaceRelatedRecords(
    op: ReplaceRelatedRecordsOperation,
    trx: Transaction
  ) {
    const { type, id } = op.record;
    const qb = this.queryForType(trx, type).context({
      recordId: id,
    });
    const relatedIds = op.relatedRecords.map(({ id }) => id);

    const model = await qb.upsertGraph(
      {
        id,
        [op.relationship]: relatedIds.map((id) => ({ id })),
      },
      {
        insertMissing: false,
        relate: false,
        unrelate: true,
      }
    );

    return model.toOrbitRecord();
  }

  protected async addToRelatedRecords(
    op: AddToRelatedRecordsOperation,
    trx: Transaction
  ) {
    const { type, id } = op.record;
    const qb = this.queryForType(trx, type).context({
      recordId: id,
    });
    const relatedId = op.relatedRecord.id;

    const model = (await qb.findById(id)) as BaseModel;
    await model.$relatedQuery(op.relationship, trx).relate(relatedId);

    return model.toOrbitRecord();
  }

  protected async removeFromRelatedRecords(
    op: RemoveFromRelatedRecordsOperation,
    trx: Transaction
  ) {
    const { type, id } = op.record;
    const qb = this.queryForType(trx, type).context({
      recordId: id,
    });

    const model = (await qb.findById(id)) as BaseModel;
    const relatedId = op.relatedRecord.id;

    await model
      .$relatedQuery(op.relationship, trx)
      .unrelate()
      .where('id', relatedId);
    return model.toOrbitRecord();
  }

  protected async findRecord(expression: FindRecord, trx: Transaction) {
    const { id, type } = expression.record;
    const qb = this.queryForType(trx, type).context({
      recordId: id,
    });

    const model = (await qb.findById(id)) as BaseModel;

    return model.toOrbitRecord();
  }

  protected async findRecords(expression: FindRecords, trx: Transaction) {
    const { type, records } = expression;
    if (type) {
      const qb = this.queryForType(trx, type, false);
      const models = (await this.parseQueryExpression(
        qb,
        expression
      )) as BaseModel[];
      return models.map((model) => model.toOrbitRecord());
    } else if (records) {
      const recordsByType = groupRecordsByType(records);
      const recordsById: Record<string, OrbitRecord> = {};

      for (let type in recordsByType) {
        for (let record of await this.queryForType(trx, type, false).findByIds(
          recordsByType[type]
        )) {
          recordsById[record.id] = record.toOrbitRecord();
        }
      }
      return records
        .map(({ id }) => recordsById[id])
        .filter((record) => record);
    }
    throw new QueryExpressionParseError(
      `FindRecords with no type or records is not recognized for SQLSource.`,
      expression
    );
  }

  protected async findRelatedRecord(
    expression: FindRelatedRecord,
    trx: Transaction
  ) {
    const {
      record: { id, type },
      relationship,
    } = expression;

    let qb = this.queryForType(trx, type).context({
      recordId: id,
    });
    const parent = (await qb.findById(id)) as BaseModel;
    qb = this.queryForRelationship(trx, parent, relationship);
    const model = ((await qb) as any) as BaseModel | undefined;

    return model ? model.toOrbitRecord() : null;
  }

  protected async findRelatedRecords(
    expression: FindRelatedRecords,
    trx: Transaction
  ) {
    const {
      record: { id, type },
      relationship,
    } = expression;

    let qb = this.queryForType(trx, type).context({
      recordId: id,
    });
    const parent = (await qb.findById(id)) as BaseModel;
    const models = (await this.parseQueryExpression(
      this.queryForRelationship(trx, parent, relationship),
      expression
    )) as BaseModel[];

    return models.map((model) => model.toOrbitRecord());
  }

  modelForType(type: string): ModelClass<BaseModel> {
    return this._models[type];
  }

  queryForType(trx: Transaction, type: string, throwIfNotFound = true) {
    const fields = this.fieldsForType(type);

    const qb = this.modelForType(type)
      .query(trx)
      .context({ recordType: type })
      .select(fields);

    if (throwIfNotFound) {
      return qb.throwIfNotFound();
    }

    return qb;
  }

  queryForRelationship(
    trx: Transaction,
    model: BaseModel,
    relationship: string
  ) {
    const relDef = this.schema.getRelationship(model.orbitType, relationship);
    const fields = this.fieldsForType(relDef?.type as string);

    return model.$relatedQuery<BaseModel>(relationship, trx).select(fields);
  }

  protected parseQueryExpressionPage(
    qb: QueryBuilder<BaseModel>,
    expression: FindRecords | FindRelatedRecords
  ) {
    if (expression.page) {
      if (expression.page.kind === 'offsetLimit') {
        const offsetLimitPage = expression.page as OffsetLimitPageSpecifier;
        if (offsetLimitPage.limit) {
          qb = qb.limit(offsetLimitPage.limit);
        }
        if (offsetLimitPage.offset) {
          qb = qb.offset(offsetLimitPage.offset);
        }
      } else {
        throw new QueryExpressionParseError(
          `Page specifier ${expression.page.kind} not recognized for SQLSource.`,
          expression
        );
      }
    }

    return qb;
  }

  protected parseQueryExpressionSort(
    qb: QueryBuilder<BaseModel>,
    expression: FindRecords | FindRelatedRecords
  ) {
    if (expression.sort) {
      for (let sortSpecifier of expression.sort) {
        if (sortSpecifier.kind === 'attribute') {
          const attributeSort = sortSpecifier as AttributeSortSpecifier;
          if (sortSpecifier.order === 'descending') {
            qb = qb.orderBy(attributeSort.attribute, 'desc');
          } else {
            qb = qb.orderBy(attributeSort.attribute);
          }
        } else {
          throw new QueryExpressionParseError(
            `Sort specifier ${sortSpecifier.kind} not recognized for SQLSource.`,
            expression
          );
        }
      }
    }

    return qb;
  }

  protected parseQueryExpressionFilter(
    qb: QueryBuilder<BaseModel>,
    expression: FindRecords | FindRelatedRecords
  ) {
    if (expression.filter) {
      for (let filterSpecifier of expression.filter) {
        if (filterSpecifier.kind === 'attribute') {
          const attributeFilter = filterSpecifier as AttributeFilterSpecifier;
          switch (attributeFilter.op) {
            case 'equal':
              qb = qb.where(attributeFilter.attribute, attributeFilter.value);
              break;
            case 'gt':
              qb = qb.where(
                attributeFilter.attribute,
                '>',
                attributeFilter.value
              );
              break;
            case 'lt':
              qb = qb.where(
                attributeFilter.attribute,
                '<',
                attributeFilter.value
              );
              break;
            case 'gte':
              qb = qb.where(
                attributeFilter.attribute,
                '>=',
                attributeFilter.value
              );
              break;
            case 'lte':
              qb = qb.where(
                attributeFilter.attribute,
                '<=',
                attributeFilter.value
              );
              break;
          }
        }
      }
    }

    return qb;
  }

  protected parseQueryExpression(
    qb: QueryBuilder<BaseModel>,
    expression: FindRecords | FindRelatedRecords
  ) {
    qb = this.parseQueryExpressionSort(qb, expression);
    qb = this.parseQueryExpressionFilter(qb, expression);
    return this.parseQueryExpressionPage(qb, expression);
  }

  protected parseOrbitRecord(record: OrbitRecord) {
    const properties: Record<string, unknown> = {};

    if (record.id) {
      properties.id = record.id;
    }

    if (record.attributes) {
      this.schema.eachAttribute(record.type, (property) => {
        if (record.attributes && record.attributes[property] !== undefined) {
          properties[property] = record.attributes[property];
        }
      });
    }

    if (record.relationships) {
      this.schema.eachRelationship(record.type, (property, { kind }) => {
        if (record.relationships && record.relationships[property]) {
          if (kind === 'hasOne') {
            const data = record.relationships[property]
              .data as RecordIdentity | null;
            properties[property] = data ? { id: data.id } : null;
          } else if (kind === 'hasMany') {
            const data = record.relationships[property]
              .data as RecordIdentity[];
            properties[property] = data.map(({ id }) => ({ id }));
          }
        }
      });
    }

    return properties;
  }

  protected fieldsForType(type: string) {
    const tableName = tableize(type);
    const fields: string[] = [`${tableName}.id`];

    this.schema.eachAttribute(type, (property) => {
      fields.push(`${tableName}.${underscore(property)}`);
    });

    this.schema.eachRelationship(type, (property, { kind }) => {
      if (kind === 'hasOne') {
        fields.push(`${tableName}.${foreignKey(property)}`);
      }
    });

    return fields;
  }
}
