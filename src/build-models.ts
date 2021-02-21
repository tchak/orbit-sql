import {
  Model,
  ModelClass,
  RelationMapping,
  snakeCaseMappers,
} from 'objection';
import {
  RecordSchema,
  RecordNotFoundException,
  RecordRelationship,
  Record as OrbitRecord,
} from '@orbit/records';
import { foreignKey, tableize } from 'inflected';

import { tableizeJoinTable, castAttributeValue } from './utils';

export abstract class BaseModel extends Model {
  id: string;
  createdAt: string;
  updatedAt: string;

  static get virtualAttributes() {
    return ['orbitSchema', 'orbitType'];
  }
  abstract get orbitSchema(): RecordSchema;
  abstract get orbitType(): string;

  $beforeInsert() {
    this.createdAt = new Date().toISOString();
  }

  $beforeUpdate() {
    this.updatedAt = new Date().toISOString();
  }

  static get columnNameMappers() {
    return snakeCaseMappers();
  }

  static createNotFoundError() {
    const context = arguments[0];
    const type = (context && context.recordType) || 'unknown type';
    const id = (context && context.recordId) || 'unknown id';
    const error = new RecordNotFoundException(type, id);
    return error as any;
  }

  toOrbitRecord() {
    const attributes: Record<string, any> = {};
    const relationships: Record<string, RecordRelationship> = {};
    const { orbitType: type, orbitSchema: schema } = this;
    const result = this.toJSON() as any;
    const record: OrbitRecord = {
      type,
      id: result.id,
    };

    schema.eachAttribute(type, (property, attribute) => {
      if (result[property] != null) {
        attributes[property] = castAttributeValue(
          result[property],
          attribute.type
        );
        record.attributes = attributes;
      }
    });

    schema.eachRelationship(type, (property, { kind, type }) => {
      if (kind === 'hasOne') {
        const id = result[`${property}Id`] as string | undefined;
        if (id) {
          relationships[property] = {
            data: {
              type: type as string,
              id: id,
            },
          };
          record.relationships = relationships;
        }
      }
    });

    return record;
  }
}

export function buildModels(
  schema: RecordSchema
): Record<string, ModelClass<BaseModel>> {
  const models: Record<string, ModelClass<BaseModel>> = {};

  for (let type in schema.models) {
    buildModel(schema, type, models);
  }

  return models;
}

export function buildModel(
  schema: RecordSchema,
  type: string,
  models: Record<string, ModelClass<BaseModel>>
): ModelClass<BaseModel> {
  if (!models[type]) {
    const tableName = tableize(type);

    models[type] = class extends BaseModel {
      get orbitType() {
        return type;
      }
      get orbitSchema() {
        return schema;
      }

      static get tableName() {
        return tableName;
      }

      static get relationMappings() {
        const relationMappings: Record<string, RelationMapping<BaseModel>> = {};
        schema.eachRelationship(type, (property, { kind, type, inverse }) => {
          if (!inverse || !type) {
            throw new Error(
              `SQLSource: "type" and "inverse" are required on a relationship`
            );
          }

          if (Array.isArray(type)) {
            throw new Error(
              `SQLSource: polymorphic types are not supported yet`
            );
          }

          const relationColumnName = foreignKey(property);
          const inverseColumnName = foreignKey(inverse);
          const relationTableName = tableize(type);
          const relationModel = buildModel(schema, type, models);
          let relationMapping: RelationMapping<BaseModel>;

          if (kind === 'hasOne') {
            relationMapping = {
              relation: Model.BelongsToOneRelation,
              modelClass: relationModel,
              join: {
                from: `${tableName}.${relationColumnName}`,
                to: `${relationTableName}.id`,
              },
            };
          } else {
            const relDef = schema.getRelationship(type, inverse);

            if (relDef?.kind === 'hasMany') {
              const joinTableName = tableizeJoinTable(property, inverse);

              relationMapping = {
                relation: Model.ManyToManyRelation,
                modelClass: relationModel,
                join: {
                  from: `${tableName}.id`,
                  through: {
                    from: `${joinTableName}.${relationColumnName}`,
                    to: `${joinTableName}.${inverseColumnName}`,
                  },
                  to: `${relationTableName}.id`,
                },
              };
            } else {
              relationMapping = {
                relation: Model.HasManyRelation,
                modelClass: relationModel,
                join: {
                  from: `${tableName}.id`,
                  to: `${relationTableName}.${inverseColumnName}`,
                },
              };
            }
          }

          relationMappings[property] = relationMapping;
        });

        return relationMappings;
      }
    };
  }

  return models[type];
}
