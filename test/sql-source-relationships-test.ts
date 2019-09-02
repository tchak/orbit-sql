import { Schema } from '@orbit/data';

import SQLSource from '../src';

QUnit.module('SQLSource', function(hooks) {
  let schema: Schema;
  let source: SQLSource;

  hooks.beforeEach(async function() {
    schema = new Schema({
      models: {
        user: {
          attributes: {
            name: { type: 'string' }
          },
          relationships: {
            blogs: {
              type: 'hasMany',
              model: 'blog',
              inverse: 'user'
            }
          }
        },
        blog: {
          attributes: {
            title: { type: 'string' }
          },
          relationships: {
            user: {
              type: 'hasOne',
              model: 'user',
              inverse: 'blogs'
            },
            tags: {
              type: 'hasMany',
              model: 'tag',
              inverse: 'blogs'
            }
          }
        },
        tag: {
          attributes: {
            name: { type: 'string' }
          },
          relationships: {
            blogs: {
              type: 'hasMany',
              model: 'blog',
              inverse: 'tags'
            }
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

  QUnit.test('relationships', async function(assert) {
    let users = await source.query(q => q.findRecords('user'));
    assert.deepEqual(users, []);

    let user: any = {
      type: 'user',
      attributes: { name: 'Paul' },
      relationships: {}
    };
    let blog: any = { type: 'blog', attributes: { title: 'Hello World' } };
    let tag: any = { type: 'tag', attributes: { name: 'js' } };

    blog = await source.update(t => t.addRecord(blog));
    tag = await source.update(t => t.addRecord(tag));
    user.relationships.blogs = { data: [blog] };
    user = await source.update(t => t.addRecord(user));
    assert.equal(user.attributes.name, 'Paul');
    users = await source.query(q => q.findRecords('user'));
    assert.equal(users[0].attributes.name, 'Paul');
    let blogs = await source.query(q => q.findRelatedRecords(user, 'blogs'));
    user = await source.query(q => q.findRelatedRecord(blog, 'user'));
    assert.equal(user.attributes.name, 'Paul');
    assert.equal(blogs[0].attributes.title, 'Hello World');

    blog.relationships = { tags: { data: [tag] } };
    await source.update(t => t.updateRecord(blog));
    let tags = await source.query(q => q.findRelatedRecords(blog, 'tags'));
    blogs = await source.query(q => q.findRelatedRecords(tag, 'blogs'));

    assert.equal(tags[0].attributes.name, 'js');
    assert.equal(blogs[0].attributes.title, 'Hello World');
  });
});
