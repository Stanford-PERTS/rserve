import DBMigrate from 'db-migrate';
import open from 'open';
import path from 'path';

const child_process = require('child_process');
const util = require('util');
const exec = util.promisify(child_process.exec);

import getKind from '../triton/src/utils/getKind';

export const config = {
  neptune: {
    dbName: 'neptune-test-rserve',
    migrationsDirectory: '../migrations/neptune',
    migrationsConfig: '../migrations/neptune.json',
    migrationsConfigNoDb: '../migrations/nodatabase.json',
  },
  saturn: {
    dbName: 'saturn-test-rserve',
    migrationsDirectory: '../migrations/saturn',
    migrationsConfig: '../migrations/saturn.json',
    migrationsConfigNoDb: '../migrations/nodatabase.json',
  },
  triton: {
    dbName: 'triton-test-rserve',
    migrationsDirectory: '../migrations/triton',
    migrationsConfig: '../migrations/triton.json',
    migrationsConfigNoDb: '../migrations/nodatabase.json',
  },
};

import { insertNeptune, neptuneSetup, neptuneTeardown } from './dbNeptune';
import { insertResponse } from './dbSaturn';
import { insertTriton, tritonSetup, tritonTeardown } from './dbTriton';

const RSERVE_TEST_DATA_DIR = 'data';

async function makeOutputDirectory() {
  // await exec(`mkdir -p ${RSERVE_TEST_DATA_DIR}`).unref();
  await exec(`mkdir -p ${RSERVE_TEST_DATA_DIR}`);
  await exec(`rm -f ${RSERVE_TEST_DATA_DIR}/*`);
}

function createDbMigrateOptions(options, noDatabase = false) {
  return {
    // Must use a separate migration config file that does not contain the
    // "database" property or else db-migrate will attempt to connect to an
    // non-existant database with that name.
    config: noDatabase
      ? path.join(__dirname, options.migrationsConfigNoDb)
      : path.join(__dirname, options.migrationsConfig),
    cmdOptions: {
      // db-migrate will assume the migrations file folder is located at
      // __dirname/migrations if you don't tell it otherwise.
      'migrations-dir': path.join(__dirname, options.migrationsDirectory),
    },
    // https://github.com/db-migrate/node-db-migrate/issues/421#issuecomment-547307044
    throwUncatched: true,
  };
}

async function createDatabase(options) {
  const dbmigrateOptions = createDbMigrateOptions(options, true);
  const dbmigrate = DBMigrate.getInstance(true, dbmigrateOptions);
  dbmigrate.silence(true);
  await dbmigrate.createDatabase(options.dbName);
}

async function dropDatabase(options) {
  const dbmigrateOptions = createDbMigrateOptions(options, true);
  const dbmigrate = DBMigrate.getInstance(true, dbmigrateOptions);
  dbmigrate.silence(true);
  await dbmigrate.dropDatabase(options.dbName);
}

async function migrateDatabase(options) {
  const dbmigrateOptions = createDbMigrateOptions(options);
  const dbmigrate = DBMigrate.getInstance(true, dbmigrateOptions);
  dbmigrate.silence(true);
  await dbmigrate.reset().then(() => dbmigrate.up());
}

export async function initialize() {
  // Output directory for rserve json data files
  await makeOutputDirectory();

  // Create & Run Initial Database Migrations
  await createDatabase(config.neptune);
  await createDatabase(config.saturn);
  await createDatabase(config.triton);
  await migrateDatabase(config.neptune);
  await migrateDatabase(config.saturn);
  await migrateDatabase(config.triton);

  await tritonSetup();
  await neptuneSetup();
}

export async function cleanup() {
  // It's very convenient to leave data in the db after a test for inspection,
  // so **don't** drop the databases.
  //// await dropDatabase(config.neptune);
  //// await dropDatabase(config.saturn);
  //// await dropDatabase(config.triton);

  await tritonTeardown();
  await neptuneTeardown();
}

export async function runR(script, reportingUnitIds, renderWithTemplate) {
  const jsonIds = JSON.stringify(reportingUnitIds);
  if (jsonIds.includes(' ')) {
    // Note: JSON _shouldn't_ put any spaces in here, but if it did it would
    // break the command line.
    throw new Error(`Space found in reporting unit ids: ${jsonIds}`);
  }
  // This will be interpreted by bash, so escape double quotes.
  const jsonEscaped = jsonIds.replace(/"/g, '\\"');
  const rCommand = `RScript test/rserve_script_runner.R ${script} ${jsonEscaped}`;
  // eslint-disable-next-line no-console
  console.log('Debug RServe with command: \n', `${rCommand} show_logs`);
  // Also available to destructure: error, stderr
  const fiveMebibytes = 5 * 1024 * 1024;
  const { stdout } = await exec(rCommand, { maxBuffer: fiveMebibytes });
  const datasetsById = JSON.parse(stdout);

  // We often want to SEE the reports generated. Take advantage of the fact
  // rserve_script_runner.R saves datasets to disk and run them through jinja
  // and open a browser.
  if (renderWithTemplate) {
    const templatePath = `test/neptune/templates/${renderWithTemplate}.html`;
    const orgTemplatePath = `test/neptune/templates/${renderWithTemplate}_organization.html`;
    for (const reportingUnitId of Object.keys(datasetsById)) {
      const dataFilePath = `test/${RSERVE_TEST_DATA_DIR}/${reportingUnitId}.json`;
      const htmlFilePath = `${RSERVE_TEST_DATA_DIR}/${reportingUnitId}.html`;
      const tPath =
        getKind(reportingUnitId) === 'Organization'
          ? orgTemplatePath
          : templatePath;
      const renderCommand =
        `test/render_neptune_template.py ` +
        `--data=${dataFilePath} ` +
        `--out=${htmlFilePath} ` +
        `--template=${tPath}`;
      await exec(renderCommand);
      await open(htmlFilePath);
    }
  }

  return datasetsById;
}

export async function insertData({
  organizations = [],
  teams = [],
  classrooms = [],
  cycles = [],
  programs = [],
  tritonParticipants = [],
  neptuneParticipants = [],
  responses = [],
  users = [],
}) {
  for (const org of organizations) {
    const orgToInsert = { ...org };
    await insertTriton('organization', orgToInsert);
  }

  for (const team of teams) {
    const teamToInsert = { ...team };
    delete teamToInsert.num_users;
    await insertTriton('team', teamToInsert);
  }

  for (const classroom of classrooms) {
    await insertTriton('classroom', classroom);
  }

  for (const cycle of cycles) {
    await insertTriton('cycle', cycle);
  }

  for (const program of programs) {
    await insertTriton('program', program);
  }

  for (const participant of tritonParticipants) {
    // Remove the classroom_code convenience property before attempting to
    // insert the participant into the database.
    const participantToInsert = { ...participant };
    delete participantToInsert.classroom_code;

    await insertTriton('participant', participantToInsert);
  }

  for (const participant of neptuneParticipants) {
    await insertNeptune('participant', participant);
  }

  for (const response of responses) {
    await insertResponse(response, response.firestore_id);
  }

  for (const user of users) {
    await insertTriton('user', user);
  }
}
