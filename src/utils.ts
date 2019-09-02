import {
  Record as OrbitRecord,
  Schema,
  RecordRelationship,
  RecordIdentity
} from '@orbit/data';
import { tableize, underscore, foreignKey } from 'inflected';
import { BaseModel } from './build-models';

export function tableizeJoinTable(table1: string, table2: string) {
  return [tableize(table1), tableize(table2)].sort().join('_');
}

export function toOrbitRecord(model: BaseModel): OrbitRecord {
  const record: OrbitRecord = {
    type: model.orbitType,
    id: model.id
  };
  const attributes: Record<string, any> = {};
  const relationships: Record<string, RecordRelationship> = {};
  const { orbitType: type, orbitSchema: schema } = model;

  const result = model.toJSON() as any;
  schema.eachAttribute(type, (property, attribute) => {
    if (result[property] != null) {
      (attributes as Record<string, unknown>)[property] = castAttributeValue(
        result[property],
        attribute.type
      );
      record.attributes = attributes;
    }
  });

  schema.eachRelationship(type, (property, { type: kind, model: type }) => {
    if (kind === 'hasOne') {
      const id = result[`${property}Id`];
      if (id) {
        (relationships as Record<string, unknown>)[property] = {
          data: {
            type: type as string,
            id: id as string
          }
        };
        record.relationships = relationships;
      }
    }
  });

  return record;
}

export function castAttributeValue(value: unknown, type?: string) {
  const typeOfValue = typeof value;
  const isString = typeOfValue === 'string';
  const isNumber = typeOfValue === 'number';
  if (type === 'boolean') {
    return value === 1;
  } else if (type === 'datetime' && (isString || isNumber)) {
    return new Date(value as string | number);
  }
  return value;
}

export function fieldsForType(schema: Schema, type: string) {
  const tableName = tableize(type);
  const fields: string[] = [`${tableName}.id`];

  schema.eachAttribute(type, property => {
    fields.push(`${tableName}.${underscore(property)}`);
  });

  schema.eachRelationship(type, (property, { type: kind }) => {
    if (kind === 'hasOne') {
      fields.push(`${tableName}.${foreignKey(property)}`);
    }
  });

  return fields;
}

export function toJSON(record: OrbitRecord, schema: Schema) {
  const properties: Record<string, unknown> = {
    id: record.id
  };

  if (record.attributes) {
    schema.eachAttribute(record.type, property => {
      if (record.attributes && record.attributes[property] !== undefined) {
        properties[property] = record.attributes[property];
      }
    });
  }

  if (record.relationships) {
    schema.eachRelationship(record.type, (property, { type: kind }) => {
      if (record.relationships && record.relationships[property]) {
        if (kind === 'hasOne') {
          const data = record.relationships[property]
            .data as RecordIdentity | null;
          properties[property] = data ? { id: data.id } : null;
        } else {
          const data = record.relationships[property].data as RecordIdentity[];
          properties[property] = data.map(({ id }) => ({ id }));
        }
      }
    });
  }

  return properties;
}
