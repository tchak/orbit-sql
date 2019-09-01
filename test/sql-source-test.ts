import { Schema } from '@orbit/data';

import SQLSource from '../src';

QUnit.module('sql source', function(hooks) {
  let schema: Schema;
  let source: SQLSource;

  hooks.beforeEach(async function() {
    schema = new Schema({
      models: {
        user: {
          attributes: {
            name: { type: 'string' }
          }
        }
      }
    });

    source = new SQLSource({
      schema,
      knex: {
        client: 'sqlite3',
        connection: { filename: ':memory:' },
        useNullAsDefault: true
      }
    });
    await source.activated;
  });

  hooks.afterEach(async function() {
    await source.deactivate();
  });

  QUnit.test('it exists', function(assert) {
    assert.ok(source);
    assert.ok(source instanceof SQLSource);
  });
});
