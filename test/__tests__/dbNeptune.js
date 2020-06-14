import * as mysql from 'promise-mysql';

const MYSQL_PASSWORD = process.env.MYSQL_PASSWORD || '';

export async function createDatabase(db) {
  const connection = await mysql.createConnection({
    host: 'localhost',
    user: 'root',
    password: MYSQL_PASSWORD,
  });

  const query = `CREATE DATABASE IF NOT EXISTS \`${db}\`;`;
  await connection.query(query);

  await connection.end();
}

export async function addTables(db) {
  const connection = await mysql.createConnection({
    host: 'localhost',
    user: 'root',
    password: MYSQL_PASSWORD,
    database: db,
  });

  await connection.end();
}

export async function dropDatabase(db) {
  const connection = await mysql.createConnection({
    host: 'localhost',
    user: 'root',
    password: MYSQL_PASSWORD,
  });

  const query = `DROP DATABASE IF EXISTS \`${db}\`;`;
  await connection.query(query);

  await connection.end();
}

import forEach from 'lodash/forEach';

const isObject = obj => typeof obj === 'object' && obj !== null;

const dbName = process.env.NEPTUNE_DB_NAME
  ? { database: process.env.NEPTUNE_DB_NAME }
  : {};
const dbUser = process.env.NEPTUNE_DB_USER
  ? { user: process.env.NEPTUNE_DB_USER }
  : {};
const dbPass = process.env.NEPTUNE_DB_PASS
  ? { password: process.env.NEPTUNE_DB_PASS }
  : {};

// Use a single knex connection so it can pool.
let knex;

// If using with jest, then call in `beforeEach` or `beforeAll`.
export function neptuneSetup() {
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
export function neptuneTeardown() {
  if (knex) {
    knex.destroy();
  }
}

// Example usage, add a team to neptune `team` table:

export async function insertNeptune(tableName, entity) {
  // Array and Objects are stored as '[]' and '{}', so JSON stringify them.
  forEach(entity, (value, prop) => {
    if (isObject(value)) {
      entity[prop] = JSON.stringify(value);
    }
  });

  if (!knex) {
    neptuneSetup();
  }

  // http://knexjs.org/#Builder-insert
  await knex(tableName).insert(entity);
}
