
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

async function getwrongTableNames(configTables) {
  const tables = await database.getAllTableNames();
  const wrongTableNames = [];
  for (const configTable of configTables) {
    wrongTableNames.push(configTable.fullName);
    for (const table of tables) {
      if (table.schema === configTable.schema && table.name === configTable.name) {
        wrongTableNames.pop();
        break;
      }
    }
  }
  return wrongTableNames;
}

async function getMissingTriggers(table) {
  const triggers = await database.getTableTriggers(table.name, table.schema);
  const missingTriggers = new Set(Object.values(usedTriggers))
  for (const {triggerName } of triggers) {
    console.log(triggerName, table.fullName)
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
  console.log(missingTriggers)
  const promises = []
  for (let i = 0; i < configTables.length; i++) {
    const triggerPromises = missingTriggers[i].map(triggerLabel => createTrigger(configTables[i], triggerLabel))
    Array.prototype.push.apply(promises, triggerPromises);
  }
  return Promise.all(promises);
}

function listenToChanges(handlers) {
  // const promises = [
  //   database.notificationListen(channels.update, (data) => handlers.onUpdate(JSON.parse(data.payload))),
  //   database.notificationListen(channels.insert, (data) => handlers.onInsert(JSON.parse(data.payload))),
  //   database.notificationListen(channels.delete, (data) => handlers.onDelete(JSON.parse(data.payload))),
  // ];

  return database.notificationListen([
    channels.update, 
    channels.insert, 
    channels.delete,
  ], (event) => {
    const payload = JSON.parse(event.payload);
    console.log("CHANNEL TRIGGER " + event.channel)
    switch(event.channel) {
      case 'table_watcher_insert': handlers.onInsert(payload); break; 
      case 'table_watcher_update': handlers.onUpdate(payload); break; 
      case 'table_watcher_delete': handlers.onDelete(payload); break; 
    }
  });
}

(
  async () => {
    const configTables = separateTablenames(prependPublic(config.tables));
    const wrongTableNames = await getwrongTableNames(configTables);
    if(wrongTableNames.length !== 0) {
      throw new Error('The following tables weren\'t found in the database: ' + wrongTableNames.join(', '));
    }
    await database.declareFunctions(channels.delete, channels.update, channels.insert);
    console.log("BEFORE CREATING TRIGGERS")
    await createNecessaryTriggers(configTables);
    console.log("AFTER CREATING TRIGGERS")
    await listenToChanges(saver);
  }
)();