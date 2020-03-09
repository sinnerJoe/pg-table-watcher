
const database = require('./database');
const config = require('../config');
const saver = require('./data-saver');
const { usedTriggers } = require('./constants');
const { channels } = config;

function prependPublic(tables) {
  return tables.map(table => (table.indexOf('.') === -1 ? `public.${table}` : table));
}

function separateTablenames(tables) {
  return tables.map((table) => {
    const [schema, name] = table.split('.');
    return { schema, name, fullName: `${schema}.${name}` };
  })
}

async function getwrongTableNames(existingTables, configTables) {
  const wrongTableNames = [];
  for (const configTable of configTables) {
    wrongTableNames.push(configTable.fullName);
    for (const table of existingTables) {
      if (table.schema === configTable.schema && table.name === configTable.name) {
        wrongTableNames.pop();
        break;
      }
    }
  }
  return wrongTableNames;
}

function getTablesFromSchemas(allTables, schemaSet) {
  return allTables.filter(table => schemaSet.has(table.schema));
}

async function getMissingTriggers(table) {
  const triggers = await database.getTableTriggers(table.name, table.schema);
  const missingTriggers = new Set(Object.values(usedTriggers))
  for (const {triggerName } of triggers) {
    // console.log(triggerName, table.fullName)
    if (usedTriggers[triggerName]) {
      missingTriggers.delete(usedTriggers[triggerName])
    }
  }
  return Array.from(missingTriggers);
}

function createTrigger(table, operation) {
  switch(operation) {
    case 'INSERT':
      return database.createInsertTrigger(table.fullName);
    case 'UPDATE':
      return database.createUpdateTrigger(table.fullName);
    case 'DELETE':
      return database.createDeleteTrigger(table.fullName);
    default:
      throw new Error(`Operation ${operation} doesn't exist`);
  }
}

async function createNecessaryTriggers(configTables){
  const missingTriggers = await Promise.all(configTables.map(getMissingTriggers));
  const promises = []
  for (let i = 0; i < configTables.length; i++) {
    const triggerPromises = missingTriggers[i].map(triggerLabel => createTrigger(configTables[i], triggerLabel))
    Array.prototype.push.apply(promises, triggerPromises);
  }
  return Promise.all(promises);
}

function listenToChanges(handlers, tables) {
  const listenedTables = new Set(tables.map(table => table.fullName));
  return database.notificationListen([
    channels.update, 
    channels.insert, 
    channels.delete,
  ], (event) => {
    const payload = JSON.parse(event.payload);
    if(!listenedTables.has(payload.table)) return;
    switch (event.channel) {
      case 'table_watcher_insert': handlers.onInsert(payload); break; 
      case 'table_watcher_update': handlers.onUpdate(payload); break; 
      case 'table_watcher_delete': handlers.onDelete(payload); break; 
    }
  });
}

async function getExistingTables() {
  const tables = await database.getAllTableNames();
  tables.forEach((table) => { table.fullName = `${table.schema}.${table.name}` });
  return tables;
}

function isStringArray(object) {
  return Array.isArray(object) && object.length && object.every(element => typeof element === 'string');
}

function addTablesToCollection(collection, tables) {
  for (const table of tables) {
    collection[table.fullName] = table;
  }
}

function removeTablesFromCollection(collection, tables) {
  for(const table of tables) {
    delete collection[table.fullName];
  }
}

(
  async () => {
    let configTables = {};
    const existingTables = await getExistingTables();
    if(isStringArray(config.tables)) {
      const tables = separateTablenames(prependPublic(config.tables));
      addTablesToCollection(configTables, tables);

      const wrongTableNames = await getwrongTableNames(existingTables, Object.values(configTables));
      if (wrongTableNames.length !== 0) {
        throw new Error('The following tables weren\'t found in the database: ' + wrongTableNames.join(', '));
      }
    }
    
    if(isStringArray(config.schemas)){
      const tables = getTablesFromSchemas(existingTables, new Set(config.schemas));
      addTablesToCollection(configTables, tables);
    }

    if(isStringArray(config.excludeTables)) {
      const tables = separateTablenames(prependPublic(config.excludeTables));
      removeTablesFromCollection(configTables, tables);
    }
    const configTableList = Object.values(configTables);
    await database.declareFunctions(channels.delete, channels.update, channels.insert);
    console.log('Functions were declared.')
    await createNecessaryTriggers(configTableList);
    console.log('Triggers were created.')
    console.log('Listening to changes.')
    await listenToChanges(saver, configTableList);
  }
)();