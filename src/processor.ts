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
  Query,
  QueryExpressionParseError,
  AttributeSortSpecifier,
  OffsetLimitPageSpecifier,
  RecordIdentity,
  AttributeFilterSpecifier
} from '@orbit/data';
import { QueryBuilder } from 'objection';

import { toJSON, fieldsForType } from './utils';
import SQLSource from './sql-source';
import { BaseModel } from './build-models';

export default class Processor {
  source: SQLSource;

  constructor(source: SQLSource) {
    this.source = source;
  }

  async patch(operations: RecordOperation[]) {
    const result: OrbitRecord[] = [];
    for (let operation of operations) {
      result.push(await this.processOperation(operation));
    }
    return result;
  }

  async query(query: Query) {
    switch (query.expression.op) {
      case 'findRecord':
        return this.findRecord(query.expression as FindRecord);
      case 'findRecords':
        return this.findRecords(query.expression as FindRecords);
      case 'findRelatedRecord':
        return this.findRelatedRecord(query.expression as FindRelatedRecord);
      case 'findRelatedRecords':
        return this.findRelatedRecords(query.expression as FindRelatedRecords);
      default:
        throw new Error(`Unknown query ${query.expression.op}`);
    }
  }

  protected processOperation(operation: RecordOperation) {
    switch (operation.op) {
      case 'addRecord':
        return this.addRecord(operation);
      case 'updateRecord':
        return this.updateRecord(operation);
      case 'removeRecord':
        return this.removeRecord(operation);
      case 'replaceAttribute':
        return this.replaceAttribute(operation);
      case 'replaceRelatedRecord':
        return this.replaceRelatedRecord(operation);
      case 'replaceRelatedRecords':
        return this.replaceRelatedRecords(operation);
      case 'addToRelatedRecords':
        return this.addToRelatedRecords(operation);
      case 'removeFromRelatedRecords':
        return this.removeFromRelatedRecords(operation);
      default:
        throw new Error(`Unknown operation ${operation.op}`);
    }
  }

  protected async addRecord(op: AddRecordOperation) {
    const qb = this.queryBuilderForType(op.record.type);
    const data = this.toJSON(op.record);

    const model = await qb.upsertGraph(data, {
      insertMissing: true,
      relate: true,
      unrelate: true
    });

    return model.toOrbitRecord();
  }

  protected async updateRecord(op: UpdateRecordOperation) {
    const qb = this.queryBuilderForType(op.record.type);
    const data = this.toJSON(op.record);

    const model = await qb.upsertGraph(data, {
      relate: true,
      unrelate: true
    });

    return model.toOrbitRecord();
  }

  protected async removeRecord(op: RemoveRecordOperation) {
    const { type, id } = op.record;
    const qb = this.queryBuilderForType(type);

    const model = (await qb.findById(id)) as BaseModel;
    await qb.deleteById(id);

    return model.toOrbitRecord();
  }

  protected async replaceAttribute(op: ReplaceAttributeOperation) {
    const { type, id } = op.record;
    const qb = this.queryBuilderForType(type);

    const model = await qb.patchAndFetchById(id, {
      [op.attribute]: op.value
    });

    return model.toOrbitRecord();
  }

  protected async replaceRelatedRecord(op: ReplaceRelatedRecordOperation) {
    const { type, id } = op.record;
    const qb = this.queryBuilderForType(type);
    const relatedId = op.relatedRecord ? op.relatedRecord.id : null;

    const model = (await qb.findById(id)) as BaseModel;
    if (relatedId) {
      await model.$relatedQuery(op.relationship).relate(relatedId);
    } else {
      await model.$relatedQuery(op.relationship).unrelate();
    }

    return model.toOrbitRecord();
  }

  protected async replaceRelatedRecords(op: ReplaceRelatedRecordsOperation) {
    const { type, id } = op.record;
    const qb = this.queryBuilderForType(type);
    const relatedIds = op.relatedRecords.map(({ id }) => id);

    const model = await qb.upsertGraph(
      {
        id,
        [op.relationship]: relatedIds.map(id => ({ id }))
      },
      {
        insertMissing: false,
        relate: false,
        unrelate: true
      }
    );

    return model.toOrbitRecord();
  }

  protected async addToRelatedRecords(op: AddToRelatedRecordsOperation) {
    const { type, id } = op.record;
    const qb = this.queryBuilderForType(type);
    const relatedId = op.relatedRecord.id;

    const model = (await qb.findById(id)) as BaseModel;
    await model.$relatedQuery(op.relationship).relate(relatedId);

    return model.toOrbitRecord();
  }

  protected async removeFromRelatedRecords(
    op: RemoveFromRelatedRecordsOperation
  ) {
    const { type, id } = op.record;
    const qb = this.queryBuilderForType(type);

    const model = (await qb.findById(id)) as BaseModel;
    const relatedId = op.relatedRecord.id;

    await model
      .$relatedQuery(op.relationship)
      .unrelate()
      .where('id', relatedId);
    return model.toOrbitRecord();
  }

  protected async findRecord(expression: FindRecord) {
    const { id, type } = expression.record;
    const qb = this.queryBuilderForType(type);

    const model = (await qb.findById(id)) as BaseModel;

    return model.toOrbitRecord();
  }

  protected async findRecords(expression: FindRecords) {
    const { type, records } = expression;
    if (type) {
      const qb = this.queryBuilderForType(type, false);
      const models = (await this.queryExpressionSpecifier(
        qb,
        expression
      )) as BaseModel[];
      return models.map(model => model.toOrbitRecord());
    } else if (records) {
      const idsByType = groupIdentitiesByType(records);
      const recordsById: Record<string, OrbitRecord> = {};

      for (let type in idsByType) {
        for (let record of await this.queryBuilderForType(
          type,
          false
        ).findByIds(idsByType[type])) {
          recordsById[record.id] = record.toOrbitRecord();
        }
      }
      return records.map(({ id }) => recordsById[id]).filter(record => record);
    }
    throw new QueryExpressionParseError(
      `FindRecords with no type or records is not recognized for SQLSource.`,
      expression
    );
  }

  protected async findRelatedRecord(expression: FindRelatedRecord) {
    const {
      record: { id, type },
      relationship
    } = expression;
    const qb = this.queryBuilderForType(type);
    const { model: relatedType } = this.source.schema.getRelationship(
      type,
      relationship
    );

    const parent = (await qb.findById(id)) as BaseModel;
    const query = await parent
      .$relatedQuery<BaseModel>(relationship)
      .select(fieldsForType(this.source.schema, relatedType as string));
    const model = ((await query) as any) as (BaseModel | undefined);

    return model ? model.toOrbitRecord() : null;
  }

  protected async findRelatedRecords(expression: FindRelatedRecords) {
    const {
      record: { id, type },
      relationship
    } = expression;
    const { model: relatedType } = this.source.schema.getRelationship(
      type,
      relationship
    );

    let qb = this.queryBuilderForType(type);
    const parent = (await qb.findById(id)) as BaseModel;
    qb = parent
      .$relatedQuery<BaseModel>(relationship)
      .select(fieldsForType(this.source.schema, relatedType as string));
    const models = (await this.queryExpressionSpecifier(
      qb,
      expression
    )) as BaseModel[];

    return models.map(model => model.toOrbitRecord());
  }

  expressionWithPage(
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
          expression.page
        );
      }
    }

    return qb;
  }

  protected queryExpressionSpecifier(
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
            sortSpecifier
          );
        }
      }
    }

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

    return this.expressionWithPage(qb, expression);
  }

  protected queryBuilderForType(type: string, throwIfNotFound = true) {
    const fields = fieldsForType(this.source.schema, type);

    const qb = this.source.cache
      .queryBuilderForType(type)
      .context({ orbitType: type })
      .select(fields);

    if (throwIfNotFound) {
      return qb.throwIfNotFound();
    }

    return qb;
  }

  protected toJSON(record: OrbitRecord) {
    return toJSON(record, this.source.schema);
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
