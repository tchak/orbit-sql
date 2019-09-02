import {
  Model,
  ModelClass,
  RelationMapping,
  snakeCaseMappers
} from 'objection';
import {
  Schema,
  RecordNotFoundException,
  RecordRelationship,
  Record as OrbitRecord
} from '@orbit/data';
import { foreignKey, tableize } from 'inflected';

import { tableizeJoinTable, castAttributeValue } from './utils';

export class BaseModel extends Model {
  id: string;
  createdAt: string;
  updatedAt: string;

  static get virtualAttributes() {
    return ['orbitSchema', 'orbitType'];
  }
  orbitSchema: Schema;
  orbitType: string;

  $beforeInsert() {
    this.createdAt = new Date().toISOString();
  }

  $beforeUpdate() {
    this.updatedAt = new Date().toISOString();
  }

  static get columnNameMappers() {
    return snakeCaseMappers();
  }

  static createNotFoundError(): Error {
    // const { type, id } = ({} as any).op.record;
    const error = new RecordNotFoundException('any', 'any');
    return error as any;
  }

  toOrbitRecord() {
    const attributes: Record<string, any> = {};
    const relationships: Record<string, RecordRelationship> = {};
    const { orbitType: type, orbitSchema: schema } = this;
    const result = this.toJSON() as any;
    const record: OrbitRecord = {
      type,
      id: result.id
    };

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
}

export function buildModels(
  schema: Schema
): Record<string, ModelClass<BaseModel>> {
  const models: Record<string, ModelClass<BaseModel>> = {};

  for (let type in schema.models) {
    buildModel(schema, type, models);
  }

  return models;
}

export function buildModel(
  schema: Schema,
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
        const relationMappings: Record<string, RelationMapping> = {};
        schema.eachRelationship(
          type,
          (property, { type: kind, model: type, inverse }) => {
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
            let relationMapping: RelationMapping;

            if (kind === 'hasOne') {
              relationMapping = {
                relation: Model.BelongsToOneRelation,
                modelClass: relationModel,
                join: {
                  from: `${tableName}.${relationColumnName}`,
                  to: `${relationTableName}.id`
                }
              };
            } else {
              const { type: inverseKind } = schema.getRelationship(
                type,
                inverse
              );

              if (inverseKind === 'hasMany') {
                const joinTableName = tableizeJoinTable(property, inverse);

                relationMapping = {
                  relation: Model.ManyToManyRelation,
                  modelClass: relationModel,
                  join: {
                    from: `${tableName}.id`,
                    through: {
                      from: `${joinTableName}.${relationColumnName}`,
                      to: `${joinTableName}.${inverseColumnName}`
                    },
                    to: `${relationTableName}.id`
                  }
                };
              } else {
                relationMapping = {
                  relation: Model.HasManyRelation,
                  modelClass: relationModel,
                  join: {
                    from: `${tableName}.id`,
                    to: `${relationTableName}.${inverseColumnName}`
                  }
                };
              }
            }

            relationMappings[property] = relationMapping;
          }
        );

        return relationMappings;
      }
    };
  }

  return models[type];
}
