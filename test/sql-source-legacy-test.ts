import {
  Record,
  RecordNotFoundException,
  Schema,
  equalRecordIdentities,
  recordsInclude,
  recordsIncludeAll
} from '@orbit/data';
import { clone } from '@orbit/utils';
import SQLSource from '../src';

const { test } = QUnit;

QUnit.config.testTimeout = 1000;

QUnit.module('SQLSource (legacy)', function(hooks) {
  let schema: Schema;
  let source: SQLSource;

  hooks.beforeEach(async function() {
    schema = new Schema({
      models: {
        planet: {
          attributes: {
            name: { type: 'string' },
            sequence: { type: 'number' },
            classification: { type: 'string' },
            atmosphere: { type: 'boolean' }
          },
          relationships: {
            moons: { type: 'hasMany', model: 'moon', inverse: 'planet' }
          }
        },
        moon: {
          attributes: {
            name: { type: 'string' }
          },
          relationships: {
            planet: { type: 'hasOne', model: 'planet', inverse: 'moons' }
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

  test('it exists', function(assert) {
    assert.ok(source);
  });

  test('it creates a `queryBuilder` if none is assigned', function(assert) {
    assert.ok(source.queryBuilder, 'queryBuilder has been instantiated');
  });

  test('creates a `transformBuilder` upon first access', function(assert) {
    assert.ok(
      source.transformBuilder,
      'transformBuilder has been instantiated'
    );
    assert.strictEqual(
      source.transformBuilder.recordInitializer,
      schema,
      'transformBuilder uses the schema to initialize records'
    );
  });

  test('#update sets data and #query retrieves it', async function(assert) {
    const earth: Record = {
      type: 'planet',
      id: '1',
      attributes: { name: 'Earth' }
    };

    await source.update(t => t.addRecord(earth));

    assert.deepEqual(
      await source.query(q => q.findRecord({ type: 'planet', id: '1' })),
      earth,
      'objects strictly match'
    );
  });

  QUnit.skip(
    '#upgrade upgrades the cache to include new models introduced in a schema',
    async function(assert) {
      let person = {
        type: 'person',
        id: '1',
        relationships: { planet: { data: { type: 'planet', id: 'earth' } } }
      };

      assert.throws(async () => await source.update(t => t.addRecord(person)));

      let models = clone(schema.models);
      models.planet.relationships.inhabitants = {
        type: 'hasMany',
        model: 'person',
        inverse: 'planet'
      };
      models.person = {
        relationships: {
          planet: { type: 'hasOne', model: 'planet', inverse: 'inhabitants' }
        }
      };

      schema.upgrade({ models });
      source.upgrade();
      await source.update(t => t.addRecord(person));
      assert.deepEqual(
        await source.query(q => q.findRecord({ type: 'person', id: '1' })),
        person,
        'records match'
      );
      assert.deepEqual(
        await source.query(q => q.findRelatedRecord(person, 'planet')),
        { type: 'planet', id: 'earth' },
        'relationship exists'
      );
    }
  );

  test('#update can not remove inexistant planet', async function(assert) {
    assert.expect(1);

    let p1 = { type: 'planet', id: '1', attributes: { name: 'Earth' } };
    let p2 = { type: 'planet', id: '2' };

    try {
      await source.update(t => [t.addRecord(p1), t.removeRecord(p2)]);
    } catch (e) {
      assert.throws(() => {
        throw e;
      }, RecordNotFoundException);
    }
  });

  QUnit.skip(
    '#update tracks refs and clears them from hasOne relationships when a referenced record is removed',
    async function(assert) {
      const jupiter: Record = {
        type: 'planet',
        id: 'p1',
        attributes: { name: 'Jupiter' },
        relationships: { moons: { data: undefined } }
      };
      const io = {
        type: 'moon',
        id: 'm1',
        attributes: { name: 'Io' },
        relationships: { planet: { data: { type: 'planet', id: 'p1' } } }
      };
      const europa: Record = {
        type: 'moon',
        id: 'm2',
        attributes: { name: 'Europa' },
        relationships: { planet: { data: { type: 'planet', id: 'p1' } } }
      };

      await source.update(t => [
        t.addRecord(jupiter),
        t.addRecord(io),
        t.addRecord(europa)
      ]);

      assert.deepEqual(
        (await source.query(q => q.findRecord({ type: 'moon', id: 'm1' })))
          .relationships.planet.data,
        { type: 'planet', id: 'p1' },
        'Jupiter has been assigned to Io'
      );
      assert.deepEqual(
        (await source.query(q => q.findRecord({ type: 'moon', id: 'm2' })))
          .relationships.planet.data,
        { type: 'planet', id: 'p1' },
        'Jupiter has been assigned to Europa'
      );

      await source.update(t => t.removeRecord(jupiter));

      assert.equal(
        await source.query(q => q.findRecord({ type: 'planet', id: 'p1' })),
        undefined,
        'Jupiter is GONE'
      );

      assert.equal(
        (await source.query(q => q.findRecord({ type: 'moon', id: 'm1' })))
          .relationships.planet.data,
        undefined,
        'Jupiter has been cleared from Io'
      );
      assert.equal(
        (await source.query(q => q.findRecord({ type: 'moon', id: 'm2' })))
          .relationships.planet.data,
        undefined,
        'Jupiter has been cleared from Europa'
      );
    }
  );

  QUnit.skip(
    '#update tracks refs and clears them from hasMany relationships when a referenced record is removed',
    async function(assert) {
      const io: Record = {
        type: 'moon',
        id: 'm1',
        attributes: { name: 'Io' },
        relationships: { planet: { data: null } }
      };
      const europa: Record = {
        type: 'moon',
        id: 'm2',
        attributes: { name: 'Europa' },
        relationships: { planet: { data: null } }
      };
      const jupiter: Record = {
        type: 'planet',
        id: 'p1',
        attributes: { name: 'Jupiter' },
        relationships: {
          moons: {
            data: [{ type: 'moon', id: 'm1' }, { type: 'moon', id: 'm2' }]
          }
        }
      };

      await source.update(t => [
        t.addRecord(io),
        t.addRecord(europa),
        t.addRecord(jupiter)
      ]);

      // assert.deepEqual(
      //   (await source.query(q => q.findRecord({ type: 'planet', id: 'p1' })))
      //     .relationships.moons.data,
      //   [{ type: 'moon', id: 'm1' }, { type: 'moon', id: 'm2' }],
      //   'Jupiter has been assigned to Io and Europa'
      // );
      assert.ok(
        recordsIncludeAll(
          await source.query(q => q.findRelatedRecords(jupiter, 'moons')),
          [io, europa]
        ),
        'Jupiter has been assigned to Io and Europa'
      );

      await source.update(t => t.removeRecord(io));

      assert.equal(
        await source.query(q => q.findRecord({ type: 'moon', id: 'm1' })),
        null,
        'Io is GONE'
      );

      await source.update(t => t.removeRecord(europa));

      assert.equal(
        await source.query(q => q.findRecord({ type: 'moon', id: 'm2' })),
        null,
        'Europa is GONE'
      );

      assert.deepEqual(
        await source.query(q =>
          q.findRelatedRecords({ type: 'planet', id: 'p1' }, 'moons')
        ),
        [],
        'Europa and Io have been cleared from Jupiter'
      );
    }
  );

  test('#update adds link to hasMany', async function(assert) {
    await source.update(t => [
      t.addRecord({ type: 'planet', id: 'p1' }),
      t.addRecord({ type: 'moon', id: 'm1' }),
      // ]);
      // await source.update(t => [
      t.addToRelatedRecords({ type: 'planet', id: 'p1' }, 'moons', {
        type: 'moon',
        id: 'm1'
      })
    ]);

    assert.deepEqual(
      await source.query(q =>
        q.findRelatedRecords({ type: 'planet', id: 'p1' }, 'moons')
      ),
      [
        {
          type: 'moon',
          id: 'm1',
          relationships: { planet: { data: { type: 'planet', id: 'p1' } } }
        }
      ],
      'relationship was added'
    );
  });

  QUnit.skip(
    "#update does not remove hasMany relationship if record doesn't exist",
    async function(assert) {
      assert.expect(1);

      source.on('transform', () => {
        assert.ok(false, 'no operations were applied');
      });

      await source.update(t =>
        t.removeFromRelatedRecords({ type: 'planet', id: 'p1' }, 'moons', {
          type: 'moon',
          id: 'moon1'
        })
      );

      assert.equal(
        await source.query(q => q.findRecord({ type: 'planet', id: 'p1' })),
        undefined,
        'planet does not exist'
      );
    }
  );

  QUnit.skip("#update adds hasOne if record doesn't exist", async function(
    assert
  ) {
    assert.expect(2);

    const tb = source.transformBuilder;
    const replacePlanet = tb.replaceRelatedRecord(
      { type: 'moon', id: 'moon1' },
      'planet',
      { type: 'planet', id: 'p1' }
    );

    const addToMoons = tb.addToRelatedRecords(
      { type: 'planet', id: 'p1' },
      'moons',
      { type: 'moon', id: 'moon1' }
    );

    let order = 0;
    source.on('transform', ({ operations }) => {
      order++;
      if (order === 1) {
        assert.deepEqual(
          operations[0],
          replacePlanet,
          'applied replacePlanet operation'
        );
      } else if (order === 2) {
        assert.deepEqual(
          operations[0],
          addToMoons,
          'applied addToMoons operation'
        );
      } else {
        assert.ok(false, 'too many ops');
      }
    });

    await source.update([replacePlanet]);
  });

  QUnit.skip(
    '#update does not add link to hasMany if link already exists',
    async function(assert) {
      assert.expect(1);

      const jupiter: Record = {
        id: 'p1',
        type: 'planet',
        attributes: { name: 'Jupiter' },
        relationships: { moons: { data: [{ type: 'moon', id: 'm1' }] } }
      };

      await source.update(t => t.addRecord(jupiter));

      source.on('transform', () => {
        assert.ok(false, 'no operations were applied');
      });

      await source.update(t =>
        t.addToRelatedRecords(jupiter, 'moons', { type: 'moon', id: 'm1' })
      );

      assert.ok(true, 'patch completed');
    }
  );

  QUnit.skip(
    "#update does not remove relationship from hasMany if relationship doesn't exist",
    async function(assert) {
      assert.expect(1);

      const jupiter: Record = {
        id: 'p1',
        type: 'planet',
        attributes: { name: 'Jupiter' }
      };

      await source.update(t => t.addRecord(jupiter));

      source.on('transform', () => {
        assert.ok(false, 'no operations were applied');
      });

      await source.update(t =>
        t.removeFromRelatedRecords(jupiter, 'moons', { type: 'moon', id: 'm1' })
      );

      assert.ok(true, 'patch completed');
    }
  );

  test('#update can add and remove to has-many relationship', async function(assert) {
    assert.expect(2);

    const jupiter: Record = { id: 'jupiter', type: 'planet' };
    await source.update(t => t.addRecord(jupiter));

    const callisto = { id: 'callisto', type: 'moon' };
    await source.update(t => t.addRecord(callisto));

    await source.update(t =>
      t.addToRelatedRecords(jupiter, 'moons', { type: 'moon', id: 'callisto' })
    );

    assert.ok(
      recordsInclude(
        await source.query(q => q.findRelatedRecords(jupiter, 'moons')),
        callisto
      ),
      'moon added'
    );

    await source.update(t =>
      t.removeFromRelatedRecords(jupiter, 'moons', {
        type: 'moon',
        id: 'callisto'
      })
    );

    assert.notOk(
      recordsInclude(
        await source.query(q => q.findRelatedRecords(jupiter, 'moons')),
        callisto
      ),
      'moon removed'
    );
  });

  test('#update can add and clear has-one relationship', async function(assert) {
    assert.expect(2);

    const jupiter: Record = { id: 'jupiter', type: 'planet' };
    await source.update(t => t.addRecord(jupiter));

    const callisto = { id: 'callisto', type: 'moon' };
    await source.update(t => t.addRecord(callisto));

    await source.update(t =>
      t.replaceRelatedRecord(callisto, 'planet', {
        type: 'planet',
        id: 'jupiter'
      })
    );

    assert.ok(
      equalRecordIdentities(
        await source.query(q => q.findRelatedRecord(callisto, 'planet')),
        jupiter
      ),
      'relationship added'
    );

    await source.update(t => t.replaceRelatedRecord(callisto, 'planet', null));

    assert.notOk(
      equalRecordIdentities(
        await source.query(q => q.findRelatedRecord(callisto, 'planet')),
        jupiter
      ),
      'relationship cleared'
    );
  });

  QUnit.skip(
    'does not replace hasOne if relationship already exists',
    async function(assert) {
      assert.expect(1);

      const europa: Record = {
        id: 'm1',
        type: 'moon',
        attributes: { name: 'Europa' },
        relationships: { planet: { data: { type: 'planet', id: 'p1' } } }
      };

      await source.update(t => t.addRecord(europa));

      source.on('patch', () => {
        assert.ok(false, 'no operations were applied');
      });

      await source.update(t =>
        t.replaceRelatedRecord(europa, 'planet', { type: 'planet', id: 'p1' })
      );

      assert.ok(true, 'patch completed');
    }
  );

  QUnit.skip(
    "does not remove hasOne if relationship doesn't exist",
    async function(assert) {
      assert.expect(1);

      const europa: Record = {
        type: 'moon',
        id: 'm1',
        attributes: { name: 'Europa' }
      };

      await source.update(t => t.addRecord(europa));

      source.on('transform', () => {
        assert.ok(false, 'no operations were applied');
      });

      await source.update(t => t.replaceRelatedRecord(europa, 'planet', null));

      assert.ok(true, 'patch completed');
    }
  );

  QUnit.skip(
    '#update removing model with a bi-directional hasOne',
    async function(assert) {
      assert.expect(5);

      const hasOneSchema = new Schema({
        models: {
          one: {
            relationships: {
              two: { type: 'hasOne', model: 'two', inverse: 'one' }
            }
          },
          two: {
            relationships: {
              one: { type: 'hasOne', model: 'one', inverse: 'two' }
            }
          }
        }
      });

      await source.deactivate();
      source = new SQLSource({
        schema: hasOneSchema
      });
      await source.activate();

      await source.update(t => [
        t.addRecord({
          id: '1',
          type: 'one',
          relationships: {
            two: { data: null }
          }
        }),
        t.addRecord({
          id: '2',
          type: 'two',
          relationships: {
            one: { data: { type: 'one', id: '1' } }
          }
        })
      ]);

      const one = await source.query(q =>
        q.findRecord({ type: 'one', id: '1' })
      );
      const two = await source.query(q =>
        q.findRecord({ type: 'two', id: '2' })
      );
      assert.ok(one, 'one exists');
      assert.ok(two, 'two exists');
      assert.deepEqual(
        one.relationships.two.data,
        { type: 'two', id: '2' },
        'one links to two'
      );
      assert.deepEqual(
        two.relationships.one.data,
        { type: 'one', id: '1' },
        'two links to one'
      );

      source.update(t => t.removeRecord(two));

      assert.equal(
        (await source.query(q => q.findRecord({ type: 'one', id: '1' })))
          .relationships.two.data,
        null,
        'ones link to two got removed'
      );
    }
  );

  test('#update merges records when "replacing" and will not stomp on attributes and relationships that are not replaced', async function(assert) {
    await source.update(t => [
      t.addRecord({
        type: 'planet',
        id: '1',
        attributes: { name: 'Earth' }
      })
    ]);

    await source.update(t => [
      t.updateRecord({
        type: 'planet',
        id: '1',
        attributes: { classification: 'terrestrial' }
      })
    ]);

    assert.deepEqual(
      await source.query(q => q.findRecord({ type: 'planet', id: '1' })),
      {
        type: 'planet',
        id: '1',
        attributes: { name: 'Earth', classification: 'terrestrial' }
      },
      'records have been merged'
    );
  });

  QUnit.skip(
    '#update can replace related records but only if they are different',
    async function(assert) {
      await source.update(t => [
        t.addRecord({
          type: 'planet',
          id: '1',
          attributes: { name: 'Earth' },
          relationships: { moons: { data: [{ type: 'moon', id: 'm1' }] } }
        })
      ]);

      let result = await source.update(t => [
        t.replaceRelatedRecords({ type: 'planet', id: '1' }, 'moons', [
          { type: 'moon', id: 'm1' }
        ])
      ]);

      assert.deepEqual(
        result,
        [],
        'nothing has changed so there are no inverse ops'
      );

      result = await source.update(t => [
        t.replaceRelatedRecords({ type: 'planet', id: '1' }, 'moons', [
          { type: 'moon', id: 'm2' }
        ])
      ]);
    }
  );

  test('#update merges records when "replacing" and _will_ replace specified attributes and relationships', async function(assert) {
    await source.update(t => [
      t.addRecord({ type: 'moon', id: 'm1' }),
      t.addRecord({ type: 'moon', id: 'm2' })
    ]);

    let earth = {
      type: 'planet',
      id: '1',
      attributes: { name: 'Earth' },
      relationships: { moons: { data: [{ type: 'moon', id: 'm1' }] } }
    };

    let jupiter = {
      type: 'planet',
      id: '1',
      attributes: { name: 'Jupiter', classification: 'terrestrial' },
      relationships: { moons: { data: [{ type: 'moon', id: 'm2' }] } }
    };

    let result = await source.update(t => t.addRecord(earth));

    delete earth.relationships;
    assert.deepEqual(result, earth);

    result = await source.update(t => t.updateRecord(jupiter));

    delete jupiter.relationships;
    assert.deepEqual(result, jupiter);

    assert.deepEqual(
      await source.query(q =>
        q.findRelatedRecords({ type: 'planet', id: '1' }, 'moons')
      ),
      [
        {
          type: 'moon',
          id: 'm2',
          relationships: {
            planet: {
              data: {
                type: 'planet',
                id: '1'
              }
            }
          }
        }
      ],
      'records have been merged'
    );
  });

  test('#query can retrieve an individual record with `record`', async function(assert) {
    let jupiter = {
      type: 'planet',
      id: 'jupiter',
      attributes: {
        name: 'Jupiter',
        classification: 'gas giant',
        atmosphere: true
      }
    };
    await source.update(t => [t.addRecord(jupiter)]);

    assert.deepEqual(
      await source.query(q => q.findRecord({ type: 'planet', id: 'jupiter' })),
      jupiter
    );
  });

  test('#query can perform a simple attribute filter by value equality', async function(assert) {
    let jupiter = {
      type: 'planet',
      id: 'jupiter',
      attributes: {
        name: 'Jupiter',
        classification: 'gas giant',
        atmosphere: true
      }
    };
    let earth = {
      type: 'planet',
      id: 'earth',
      attributes: {
        name: 'Earth',
        classification: 'terrestrial',
        atmosphere: true
      }
    };
    let venus = {
      type: 'planet',
      id: 'venus',
      attributes: {
        name: 'Venus',
        classification: 'terrestrial',
        atmosphere: true
      }
    };
    let mercury = {
      type: 'planet',
      id: 'mercury',
      attributes: {
        name: 'Mercury',
        classification: 'terrestrial',
        atmosphere: false
      }
    };

    await source.update(t => [
      t.addRecord(jupiter),
      t.addRecord(earth),
      t.addRecord(venus),
      t.addRecord(mercury)
    ]);

    assert.deepEqual(
      await source.query(q =>
        q.findRecords('planet').filter({ attribute: 'name', value: 'Jupiter' })
      ),
      [jupiter]
    );
  });

  test('#query can perform a simple attribute filter by value comparison (gt, lt, gte & lte)', async function(assert) {
    let jupiter = {
      type: 'planet',
      id: 'jupiter',
      attributes: {
        name: 'Jupiter',
        sequence: 5,
        classification: 'gas giant',
        atmosphere: true
      }
    };
    let earth = {
      type: 'planet',
      id: 'earth',
      attributes: {
        name: 'Earth',
        sequence: 3,
        classification: 'terrestrial',
        atmosphere: true
      }
    };
    let venus = {
      type: 'planet',
      id: 'venus',
      attributes: {
        name: 'Venus',
        sequence: 2,
        classification: 'terrestrial',
        atmosphere: true
      }
    };
    let mercury = {
      type: 'planet',
      id: 'mercury',
      attributes: {
        name: 'Mercury',
        sequence: 1,
        classification: 'terrestrial',
        atmosphere: false
      }
    };

    await source.update(t => [
      t.addRecord(jupiter),
      t.addRecord(earth),
      t.addRecord(venus),
      t.addRecord(mercury)
    ]);
    assert.deepEqual(
      await source.query(q => {
        let tmp = q.findRecords('planet');
        return tmp.filter({ attribute: 'sequence', value: 2, op: 'gt' });
      }),
      [jupiter, earth]
    );
    assert.deepEqual(
      await source.query(q => {
        let tmp = q.findRecords('planet');
        return tmp.filter({ attribute: 'sequence', value: 2, op: 'gte' });
      }),
      [jupiter, earth, venus]
    );
    assert.deepEqual(
      await source.query(q => {
        let tmp = q.findRecords('planet');
        return tmp.filter({ attribute: 'sequence', value: 2, op: 'lt' });
      }),
      [mercury]
    );
    assert.deepEqual(
      await source.query(q => {
        let tmp = q.findRecords('planet');
        return tmp.filter({ attribute: 'sequence', value: 2, op: 'lte' });
      }),
      [venus, mercury]
    );
  });

  QUnit.skip(
    '#query can perform relatedRecords filters with operators `equal`, `all`, `some` and `none`',
    async function(assert) {
      let jupiter = {
        type: 'planet',
        id: 'jupiter',
        attributes: {
          name: 'Jupiter',
          sequence: 5,
          classification: 'gas giant',
          atmosphere: true
        }
      };
      let earth = {
        type: 'planet',
        id: 'earth',
        attributes: {
          name: 'Earth',
          sequence: 3,
          classification: 'terrestrial',
          atmosphere: true
        }
      };
      let mars = {
        type: 'planet',
        id: 'mars',
        attributes: {
          name: 'Mars',
          sequence: 4,
          classification: 'terrestrial',
          atmosphere: true
        }
      };
      let mercury = {
        type: 'planet',
        id: 'mercury',
        attributes: {
          name: 'Mercury',
          sequence: 1,
          classification: 'terrestrial',
          atmosphere: false
        }
      };
      let theMoon = {
        id: 'moon',
        type: 'moon',
        attributes: { name: 'The moon' },
        relationships: { planet: { data: { type: 'planet', id: 'earth' } } }
      };
      let europa = {
        id: 'europa',
        type: 'moon',
        attributes: { name: 'Europa' },
        relationships: { planet: { data: { type: 'planet', id: 'jupiter' } } }
      };
      let ganymede = {
        id: 'ganymede',
        type: 'moon',
        attributes: { name: 'Ganymede' },
        relationships: { planet: { data: { type: 'planet', id: 'jupiter' } } }
      };
      let callisto = {
        id: 'callisto',
        type: 'moon',
        attributes: { name: 'Callisto' },
        relationships: { planet: { data: { type: 'planet', id: 'jupiter' } } }
      };
      let phobos = {
        id: 'phobos',
        type: 'moon',
        attributes: { name: 'Phobos' },
        relationships: { planet: { data: { type: 'planet', id: 'mars' } } }
      };
      let deimos = {
        id: 'deimos',
        type: 'moon',
        attributes: { name: 'Deimos' },
        relationships: { planet: { data: { type: 'planet', id: 'mars' } } }
      };
      let titan = {
        id: 'titan',
        type: 'moon',
        attributes: { name: 'titan' },
        relationships: {}
      };

      await source.update(t => [
        t.addRecord(jupiter),
        t.addRecord(earth),
        t.addRecord(mars),
        t.addRecord(mercury),
        t.addRecord(theMoon),
        t.addRecord(europa),
        t.addRecord(ganymede),
        t.addRecord(callisto),
        t.addRecord(phobos),
        t.addRecord(deimos),
        t.addRecord(titan)
      ]);
      assert.deepEqual(
        await source.query(q =>
          q
            .findRecords('planet')
            .filter({ relation: 'moons', records: [theMoon], op: 'equal' })
        ),
        [earth]
      );
      assert.deepEqual(
        await source.query(q =>
          q
            .findRecords('planet')
            .filter({ relation: 'moons', records: [phobos], op: 'equal' })
        ),
        []
      );
      assert.deepEqual(
        await source.query(q =>
          q
            .findRecords('planet')
            .filter({ relation: 'moons', records: [phobos], op: 'all' })
        ),
        [mars]
      );
      assert.deepEqual(
        await source.query(q =>
          q.findRecords('planet').filter({
            relation: 'moons',
            records: [phobos, callisto],
            op: 'all'
          })
        ),
        []
      );
      assert.deepEqual(
        await source.query(q =>
          q.findRecords('planet').filter({
            relation: 'moons',
            records: [phobos, callisto],
            op: 'some'
          })
        ),
        [mars, jupiter]
      );
      assert.deepEqual(
        await source.query(q =>
          q
            .findRecords('planet')
            .filter({ relation: 'moons', records: [titan], op: 'some' })
        ),
        []
      );
      assert.deepEqual(
        await source.query(q =>
          q
            .findRecords('planet')
            .filter({ relation: 'moons', records: [ganymede], op: 'none' })
        ),
        [earth, mars, mercury]
      );
    }
  );

  QUnit.skip('#query can perform relatedRecord filters', async function(
    assert
  ) {
    let jupiter = {
      type: 'planet',
      id: 'jupiter',
      attributes: {
        name: 'Jupiter',
        sequence: 5,
        classification: 'gas giant',
        atmosphere: true
      },
      relationships: {
        moons: {
          data: [
            { type: 'moon', id: 'europa' },
            { type: 'moon', id: 'ganymede' },
            { type: 'moon', id: 'callisto' }
          ]
        }
      }
    };
    let earth = {
      type: 'planet',
      id: 'earth',
      attributes: {
        name: 'Earth',
        sequence: 3,
        classification: 'terrestrial',
        atmosphere: true
      },
      relationships: { moons: { data: [{ type: 'moon', id: 'moon' }] } }
    };
    let mars = {
      type: 'planet',
      id: 'mars',
      attributes: {
        name: 'Mars',
        sequence: 4,
        classification: 'terrestrial',
        atmosphere: true
      },
      relationships: {
        moons: {
          data: [{ type: 'moon', id: 'phobos' }, { type: 'moon', id: 'deimos' }]
        }
      }
    };
    let mercury = {
      type: 'planet',
      id: 'mercury',
      attributes: {
        name: 'Mercury',
        sequence: 1,
        classification: 'terrestrial',
        atmosphere: false
      }
    };
    let theMoon = {
      id: 'moon',
      type: 'moon',
      attributes: { name: 'The moon' },
      relationships: { planet: { data: { type: 'planet', id: 'earth' } } }
    };
    let europa = {
      id: 'europa',
      type: 'moon',
      attributes: { name: 'Europa' },
      relationships: { planet: { data: { type: 'planet', id: 'jupiter' } } }
    };
    let ganymede = {
      id: 'ganymede',
      type: 'moon',
      attributes: { name: 'Ganymede' },
      relationships: { planet: { data: { type: 'planet', id: 'jupiter' } } }
    };
    let callisto = {
      id: 'callisto',
      type: 'moon',
      attributes: { name: 'Callisto' },
      relationships: { planet: { data: { type: 'planet', id: 'jupiter' } } }
    };
    let phobos = {
      id: 'phobos',
      type: 'moon',
      attributes: { name: 'Phobos' },
      relationships: { planet: { data: { type: 'planet', id: 'mars' } } }
    };
    let deimos = {
      id: 'deimos',
      type: 'moon',
      attributes: { name: 'Deimos' },
      relationships: { planet: { data: { type: 'planet', id: 'mars' } } }
    };
    let titan = {
      id: 'titan',
      type: 'moon',
      attributes: { name: 'titan' },
      relationships: {}
    };

    await source.update(t => [
      t.addRecord(jupiter),
      t.addRecord(earth),
      t.addRecord(mars),
      t.addRecord(mercury),
      t.addRecord(theMoon),
      t.addRecord(europa),
      t.addRecord(ganymede),
      t.addRecord(callisto),
      t.addRecord(phobos),
      t.addRecord(deimos),
      t.addRecord(titan)
    ]);
    assert.deepEqual(
      await source.query(q =>
        q.findRecords('moon').filter({ relation: 'planet', record: earth })
      ),
      [theMoon]
    );
    assert.deepEqual(
      await source.query(q =>
        q.findRecords('moon').filter({ relation: 'planet', record: jupiter })
      ),
      [europa, ganymede, callisto]
    );
    assert.deepEqual(
      await source.query(q =>
        q.findRecords('moon').filter({ relation: 'planet', record: mercury })
      ),
      []
    );
    assert.deepEqual(
      await source.query(q =>
        q
          .findRecords('moon')
          .filter({ relation: 'planet', record: [earth, mars] })
      ),
      [theMoon, phobos, deimos]
    );
  });

  test('#query can perform a complex attribute filter by value', async function(assert) {
    let jupiter = {
      type: 'planet',
      id: 'jupiter',
      attributes: {
        name: 'Jupiter',
        classification: 'gas giant',
        atmosphere: true
      }
    };
    let earth = {
      type: 'planet',
      id: 'earth',
      attributes: {
        name: 'Earth',
        classification: 'terrestrial',
        atmosphere: true
      }
    };
    let venus = {
      type: 'planet',
      id: 'venus',
      attributes: {
        name: 'Venus',
        classification: 'terrestrial',
        atmosphere: true
      }
    };
    let mercury = {
      type: 'planet',
      id: 'mercury',
      attributes: {
        name: 'Mercury',
        classification: 'terrestrial',
        atmosphere: false
      }
    };

    source.update(t => [
      t.addRecord(jupiter),
      t.addRecord(earth),
      t.addRecord(venus),
      t.addRecord(mercury)
    ]);

    assert.deepEqual(
      await source.query(q =>
        q
          .findRecords('planet')
          .filter(
            { attribute: 'atmosphere', value: true },
            { attribute: 'classification', value: 'terrestrial' }
          )
      ),
      [earth, venus]
    );
  });

  test('#query can perform a filter on attributes, even when a particular record has none', async function(assert) {
    let jupiter = { type: 'planet', id: 'jupiter' };
    let earth = {
      type: 'planet',
      id: 'earth',
      attributes: {
        name: 'Earth',
        classification: 'terrestrial',
        atmosphere: true
      }
    };
    let venus = {
      type: 'planet',
      id: 'venus',
      attributes: {
        name: 'Venus',
        classification: 'terrestrial',
        atmosphere: true
      }
    };
    let mercury = {
      type: 'planet',
      id: 'mercury',
      attributes: {
        name: 'Mercury',
        classification: 'terrestrial',
        atmosphere: false
      }
    };

    await source.update(t => [
      t.addRecord(jupiter),
      t.addRecord(earth),
      t.addRecord(venus),
      t.addRecord(mercury)
    ]);

    assert.deepEqual(
      await source.query(q =>
        q
          .findRecords('planet')
          .filter(
            { attribute: 'atmosphere', value: true },
            { attribute: 'classification', value: 'terrestrial' }
          )
      ),
      [earth, venus]
    );
  });

  test('#query can sort by an attribute', async function(assert) {
    let jupiter = {
      type: 'planet',
      id: 'jupiter',
      attributes: {
        name: 'Jupiter',
        classification: 'gas giant',
        atmosphere: true
      }
    };
    let earth = {
      type: 'planet',
      id: 'earth',
      attributes: {
        name: 'Earth',
        classification: 'terrestrial',
        atmosphere: true
      }
    };
    let venus = {
      type: 'planet',
      id: 'venus',
      attributes: {
        name: 'Venus',
        classification: 'terrestrial',
        atmosphere: true
      }
    };
    let mercury = {
      type: 'planet',
      id: 'mercury',
      attributes: {
        name: 'Mercury',
        classification: 'terrestrial',
        atmosphere: false
      }
    };

    await source.update(t => [
      t.addRecord(jupiter),
      t.addRecord(earth),
      t.addRecord(venus),
      t.addRecord(mercury)
    ]);

    assert.deepEqual(
      await source.query(q => q.findRecords('planet').sort('name')),
      [earth, jupiter, mercury, venus]
    );
  });

  QUnit.skip(
    '#query can sort by an attribute, even when a particular record has none',
    async function(assert) {
      let jupiter = { type: 'planet', id: 'jupiter' };
      let earth = {
        type: 'planet',
        id: 'earth',
        attributes: {
          name: 'Earth',
          classification: 'terrestrial',
          atmosphere: true
        }
      };
      let venus = {
        type: 'planet',
        id: 'venus',
        attributes: {
          name: 'Venus',
          classification: 'terrestrial',
          atmosphere: true
        }
      };
      let mercury = {
        type: 'planet',
        id: 'mercury',
        attributes: {
          name: 'Mercury',
          classification: 'terrestrial',
          atmosphere: false
        }
      };

      await source.update(t => [
        t.addRecord(jupiter),
        t.addRecord(earth),
        t.addRecord(venus),
        t.addRecord(mercury)
      ]);

      assert.deepEqual(
        await source.query(q => q.findRecords('planet').sort('name')),
        [earth, mercury, venus, jupiter]
      );
    }
  );

  test('#query can filter and sort by attributes', async function(assert) {
    let jupiter = {
      type: 'planet',
      id: 'jupiter',
      attributes: {
        name: 'Jupiter',
        classification: 'gas giant',
        atmosphere: true
      }
    };
    let earth = {
      type: 'planet',
      id: 'earth',
      attributes: {
        name: 'Earth',
        classification: 'terrestrial',
        atmosphere: true
      }
    };
    let venus = {
      type: 'planet',
      id: 'venus',
      attributes: {
        name: 'Venus',
        classification: 'terrestrial',
        atmosphere: true
      }
    };
    let mercury = {
      type: 'planet',
      id: 'mercury',
      attributes: {
        name: 'Mercury',
        classification: 'terrestrial',
        atmosphere: false
      }
    };

    await source.update(t => [
      t.addRecord(jupiter),
      t.addRecord(earth),
      t.addRecord(venus),
      t.addRecord(mercury)
    ]);

    assert.deepEqual(
      await source.query(q =>
        q
          .findRecords('planet')
          .filter(
            { attribute: 'atmosphere', value: true },
            { attribute: 'classification', value: 'terrestrial' }
          )
          .sort('name')
      ),
      [earth, venus]
    );
  });

  test('#query can sort by an attribute in descending order', async function(assert) {
    let jupiter = {
      type: 'planet',
      id: 'jupiter',
      attributes: {
        name: 'Jupiter',
        classification: 'gas giant',
        atmosphere: true
      }
    };
    let earth = {
      type: 'planet',
      id: 'earth',
      attributes: {
        name: 'Earth',
        classification: 'terrestrial',
        atmosphere: true
      }
    };
    let venus = {
      type: 'planet',
      id: 'venus',
      attributes: {
        name: 'Venus',
        classification: 'terrestrial',
        atmosphere: true
      }
    };
    let mercury = {
      type: 'planet',
      id: 'mercury',
      attributes: {
        name: 'Mercury',
        classification: 'terrestrial',
        atmosphere: false
      }
    };

    await source.update(t => [
      t.addRecord(jupiter),
      t.addRecord(earth),
      t.addRecord(venus),
      t.addRecord(mercury)
    ]);

    assert.deepEqual(
      await source.query(q => q.findRecords('planet').sort('-name')),
      [venus, mercury, jupiter, earth]
    );
  });

  test('#query can sort by according to multiple criteria', async function(assert) {
    let jupiter = {
      type: 'planet',
      id: 'jupiter',
      attributes: {
        name: 'Jupiter',
        classification: 'gas giant',
        atmosphere: true
      }
    };
    let earth = {
      type: 'planet',
      id: 'earth',
      attributes: {
        name: 'Earth',
        classification: 'terrestrial',
        atmosphere: true
      }
    };
    let venus = {
      type: 'planet',
      id: 'venus',
      attributes: {
        name: 'Venus',
        classification: 'terrestrial',
        atmosphere: true
      }
    };
    let mercury = {
      type: 'planet',
      id: 'mercury',
      attributes: {
        name: 'Mercury',
        classification: 'terrestrial',
        atmosphere: false
      }
    };

    await source.update(t => [
      t.addRecord(jupiter),
      t.addRecord(earth),
      t.addRecord(venus),
      t.addRecord(mercury)
    ]);

    assert.deepEqual(
      await source.query(q =>
        q.findRecords('planet').sort('classification', 'name')
      ),
      [jupiter, earth, mercury, venus]
    );
  });

  test('#query - findRecord - finds record', async function(assert) {
    const jupiter: Record = {
      id: 'jupiter',
      type: 'planet',
      attributes: { name: 'Jupiter' }
    };

    await source.update(t => [t.addRecord(jupiter)]);

    assert.deepEqual(
      await source.query(q => q.findRecord({ type: 'planet', id: 'jupiter' })),
      jupiter
    );
  });

  test("#query - findRecord - throws RecordNotFoundException if record doesn't exist", async function(assert) {
    try {
      await source.query(q => q.findRecord({ type: 'planet', id: 'jupiter' }));
    } catch (e) {
      assert.equal(e.message, 'Record not found: planet:jupiter');
      assert.throws(() => {
        throw e;
      }, RecordNotFoundException);
    }
  });

  test('#query - findRecords - records by type', async function(assert) {
    const jupiter: Record = {
      id: 'jupiter',
      type: 'planet',
      attributes: { name: 'Jupiter' }
    };

    const callisto = {
      id: 'callisto',
      type: 'moon',
      attributes: { name: 'Callisto' },
      relationships: { planet: { data: { type: 'planet', id: 'jupiter' } } }
    };

    await source.update(t => [t.addRecord(jupiter), t.addRecord(callisto)]);

    assert.deepEqual(await source.query(q => q.findRecords('planet')), [
      jupiter
    ]);
  });

  test('#query - findRecords - records by identity', async function(assert) {
    assert.expect(1);

    let earth: Record = {
      type: 'planet',
      id: 'earth',
      attributes: {
        name: 'Earth',
        classification: 'terrestrial'
      }
    };

    let jupiter: Record = {
      type: 'planet',
      id: 'jupiter',
      attributes: {
        name: 'Jupiter',
        classification: 'gas giant'
      }
    };

    let io: Record = {
      type: 'moon',
      id: 'io',
      attributes: {
        name: 'Io'
      }
    };

    await source.update(t => [
      t.addRecord(earth),
      t.addRecord(jupiter),
      t.addRecord(io)
    ]);

    let records = await source.query(q =>
      q.findRecords([earth, io, { type: 'moon', id: 'FAKE' }])
    );
    assert.deepEqual(records, [earth, io], 'query results are expected');
  });

  test('#query - page - can paginate records by offset and limit', async function(assert) {
    const jupiter: Record = {
      id: 'jupiter',
      type: 'planet',
      attributes: { name: 'Jupiter' }
    };

    const earth: Record = {
      id: 'earth',
      type: 'planet',
      attributes: { name: 'Earth' }
    };

    const venus = {
      id: 'venus',
      type: 'planet',
      attributes: { name: 'Venus' }
    };

    const mars = {
      id: 'mars',
      type: 'planet',
      attributes: { name: 'Mars' }
    };

    await source.update(t => [
      t.addRecord(jupiter),
      t.addRecord(earth),
      t.addRecord(venus),
      t.addRecord(mars)
    ]);

    assert.deepEqual(
      await source.query(q => q.findRecords('planet').sort('name')),
      [earth, jupiter, mars, venus]
    );

    assert.deepEqual(
      await source.query(q =>
        q
          .findRecords('planet')
          .sort('name')
          .page({ limit: 3 })
      ),
      [earth, jupiter, mars]
    );

    assert.deepEqual(
      await source.query(q =>
        q
          .findRecords('planet')
          .sort('name')
          .page({ offset: 1, limit: 2 })
      ),
      [jupiter, mars]
    );
  });

  test('#query - findRelatedRecords', async function(assert) {
    const jupiter: Record = {
      id: 'jupiter',
      type: 'planet',
      attributes: { name: 'Jupiter' }
    };

    const callisto = {
      id: 'callisto',
      type: 'moon',
      attributes: { name: 'Callisto' },
      relationships: { planet: { data: { type: 'planet', id: 'jupiter' } } }
    };

    await source.update(t => [t.addRecord(jupiter), t.addRecord(callisto)]);

    assert.deepEqual(
      await source.query(q =>
        q.findRelatedRecords({ type: 'planet', id: 'jupiter' }, 'moons')
      ),
      [callisto]
    );
  });

  test('#query - findRelatedRecord', async function(assert) {
    const jupiter: Record = {
      id: 'jupiter',
      type: 'planet',
      attributes: { name: 'Jupiter' }
    };

    const callisto = {
      id: 'callisto',
      type: 'moon',
      attributes: { name: 'Callisto' },
      relationships: { planet: { data: { type: 'planet', id: 'jupiter' } } }
    };

    await source.update(t => [t.addRecord(jupiter), t.addRecord(callisto)]);

    assert.deepEqual(
      await source.query(q =>
        q.findRelatedRecord({ type: 'moon', id: 'callisto' }, 'planet')
      ),
      jupiter
    );
  });
});
