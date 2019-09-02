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
  RecordOperation
} from '@orbit/data';
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

  processOperation(operation: RecordOperation) {
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

  async addRecord(op: AddRecordOperation) {
    const qb = this.queryBuilderForOperation(op);
    const data = this.toJSON(op.record);

    const model = await qb.upsertGraph(data, {
      insertMissing: true,
      relate: true,
      unrelate: true
    });

    return model.toOrbitRecord();
  }

  async updateRecord(op: UpdateRecordOperation) {
    const qb = this.queryBuilderForOperation(op);
    const data = this.toJSON(op.record);

    const model = await qb.upsertGraph(data, {
      relate: true,
      unrelate: true
    });

    return model.toOrbitRecord();
  }

  async removeRecord(op: RemoveRecordOperation) {
    const { id } = op.record;
    const qb = this.queryBuilderForOperation(op);

    const model = (await qb.findById(id)) as BaseModel;
    await qb.deleteById(id);

    return model.toOrbitRecord();
  }

  async replaceAttribute(op: ReplaceAttributeOperation) {
    const { id } = op.record;
    const qb = this.queryBuilderForOperation(op);

    const model = await qb.patchAndFetchById(id, {
      [op.attribute]: op.value
    });

    return model.toOrbitRecord();
  }

  async replaceRelatedRecord(op: ReplaceRelatedRecordOperation) {
    const { id } = op.record;
    const qb = this.queryBuilderForOperation(op);
    const relatedId = op.relatedRecord ? op.relatedRecord.id : null;

    const model = (await qb.findById(id)) as BaseModel;
    if (relatedId) {
      await model.$relatedQuery(op.relationship).relate(relatedId);
    } else {
      await model.$relatedQuery(op.relationship).unrelate();
    }

    return model.toOrbitRecord();
  }

  async replaceRelatedRecords(op: ReplaceRelatedRecordsOperation) {
    const { id } = op.record;
    const qb = this.queryBuilderForOperation(op);
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

  async addToRelatedRecords(op: AddToRelatedRecordsOperation) {
    const { id } = op.record;
    const qb = this.queryBuilderForOperation(op);
    const relatedId = op.relatedRecord.id;

    const model = (await qb.findById(id)) as BaseModel;
    await model.$relatedQuery(op.relationship).relate(relatedId);

    return model.toOrbitRecord();
  }

  async removeFromRelatedRecords(op: RemoveFromRelatedRecordsOperation) {
    const { id } = op.record;
    const qb = this.queryBuilderForOperation(op);

    const model = (await qb.findById(id)) as BaseModel;
    const relatedId = op.relatedRecord.id;

    await model
      .$relatedQuery(op.relationship)
      .unrelate()
      .where('id', relatedId);
    return model.toOrbitRecord();
  }

  protected queryBuilderForType(type: string) {
    return this.source.cache.queryBuilderForType(type);
  }

  protected queryBuilderForOperation(op: RecordOperation) {
    const fields = fieldsForType(this.source.schema, op.record.type);

    return this.queryBuilderForType(op.record.type)
      .context({ op })
      .throwIfNotFound()
      .select(fields);
  }

  protected toJSON(record: OrbitRecord) {
    return toJSON(record, this.source.schema);
  }
}
