import { Schema } from '@orbit/data';
import Knex from 'knex';
import { underscore, foreignKey, tableize } from 'inflected';

import { tableizeJoinTable } from './utils';

export async function migrateModels(db: Knex, schema: Schema) {
  for (let type in schema.models) {
    await migrateModel(db, schema, type);
  }
}

export async function migrateModel(db: Knex, schema: Schema, type: string) {
  const tableName = tableize(type);
  const joinTables: Record<string, [string, string]> = {};
  const hasTable = await db.schema.hasTable(tableName);

  if (hasTable) {
    return;
  }

  await db.schema.createTable(tableName, table => {
    table.uuid('id').primary();
    table.timestamps(true, true);

    schema.eachAttribute(type, (property, attribute) => {
      if (!['updatedAt', 'createdAt'].includes(property)) {
        let columnName = underscore(property);
        switch (attribute.type) {
          case 'string':
            table.string(columnName);
            break;
          case 'number':
            table.integer(columnName);
            break;
          case 'boolean':
            table.boolean(columnName);
            break;
          case 'date':
            table.date(columnName);
            break;
          case 'datetime':
            table.dateTime(columnName);
            break;
        }
      }
    });

    schema.eachRelationship(
      type,
      (property, { type: kind, model: type, inverse }) => {
        const columnName = foreignKey(property);
        if (kind === 'hasOne') {
          table.uuid(columnName);
        } else {
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

          let { type: inverseKind } = schema.getRelationship(type, inverse);

          if (inverseKind === 'hasMany') {
            joinTables[tableizeJoinTable(property, inverse)] = [
              columnName,
              foreignKey(inverse)
            ];
          }
        }
      }
    );
  });

  for (let joinTableName in joinTables) {
    if (!(await db.schema.hasTable(joinTableName))) {
      await db.schema.createTable(joinTableName, table => {
        table.uuid(joinTables[joinTableName][0]);
        table.uuid(joinTables[joinTableName][1]);
      });
    }
  }
}
