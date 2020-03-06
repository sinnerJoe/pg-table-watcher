const Pg = require('pg-promise');
const path = require('path');
const config = require('../config');

const conn = process.env[config.envConnectionString] || config.connectionString;

if (!conn) {
  throw new Error('No connection string');
}

const QueryFile = Pg.QueryFile;


const sql = new Pg({ promiseLib: Promise })(conn);

function qFile(file) {
  return new QueryFile(path.join(__dirname, '../sql', `${file}.sql`), { minify: true });
}

const sqlFiles = {
  deleteTrigger: qFile('deleteTrigger'),
  functions: qFile('functions'),
  insertTrigger: qFile('insertTrigger'),
  updateTrigger: qFile('updateTrigger'),
  getTableTriggers: qFile('getTableTriggers'),
  getAllTables: qFile('getAllTables'),
};

function throwIfFalsy(entityName, entity = 'table name') {
  if (!entityName) {
    throw new Error(`The ${entity} is undefined`);
  }
}

function declareFunctions(deleteChannel, updateChannel, insertChannel) {
  return sql.none(sqlFiles.functions, { deleteChannel, updateChannel, insertChannel });
}

function createUpdateTrigger(tableName) {
  throwIfFalsy(tableName);
  return sql.none(sqlFiles.updateTrigger, { tableName });
}

function createInsertTrigger(tableName) {
  throwIfFalsy(tableName);
  return sql.none(sqlFiles.insertTrigger, { tableName });
}
function createDeleteTrigger(tableName) {
  throwIfFalsy(tableName);
  return sql.none(sqlFiles.deleteTrigger, { tableName });
}

function getTableTriggers(table, schema) {
  throwIfFalsy(table);
  throwIfFalsy(schema, 'table schema name');
  return sql.manyOrNone(sqlFiles.getTableTriggers, { table, schema });
}

function getAllTableNames() {
  return sql.manyOrNone(sqlFiles.getAllTables);
}

async function notificationListen(channels, listener) {
  const connection = await sql.connect({ direct: true });
  connection.client.on('notification', listener);
  return Promise.all(channels.map(channel => connection.none('LISTEN $1~', channel)));
}

module.exports = {
  createInsertTrigger,
  createDeleteTrigger,
  createUpdateTrigger,
  declareFunctions,
  getTableTriggers,
  getAllTableNames,
  notificationListen,
  sql,
};
