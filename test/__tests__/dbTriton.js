import forEach from 'lodash/forEach';

const isObject = obj => typeof obj === 'object' && obj !== null;

const dbName = process.env.TRITON_DB_NAME
  ? { database: process.env.TRITON_DB_NAME }
  : {};
const dbUser = process.env.TRITON_DB_USER
  ? { user: process.env.TRITON_DB_USER }
  : {};
const dbPass = process.env.TRITON_DB_PASS
  ? { password: process.env.TRITON_DB_PASS }
  : {};

// Use a single knex connection so it can pool.
let knex;

// If using with jest, then call in `beforeEach` or `beforeAll`.
export function tritonSetup() {
  knex = require('knex')({
    client: 'mysql',
    connection: {
      host: '127.0.0.01',
      ...dbName,
      ...dbUser,
      ...dbPass,
    },
  });
}

// If using with jest, then call in `afterEach` or `afterAll`.
export function tritonTeardown() {
  if (knex) {
    knex.destroy();
  }
}

// Example usage, add a team to triton `team` table:
//
//   const team = mocks.createTeam();
//   insert('team', team);

export async function insertTriton(tableName, entity) {
  // Array and Objects are stored as '[]' and '{}', so JSON stringify them.
  forEach(entity, (value, prop) => {
    if (isObject(value)) {
      entity[prop] = JSON.stringify(value);
    }
  });

  if (!knex) {
    tritonSetup();
  }

  // http://knexjs.org/#Builder-insert
  await knex(tableName).insert(entity);
}
