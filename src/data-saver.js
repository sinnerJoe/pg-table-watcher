
const Pg = require('pg-promise');
const { sql } = require('./database');

const suffix = '_after_tbw';

let triggersEnabled = true;

const changes = [];

function addChange(change) {
  changes.push(change);
}

async function revertAllChanges() {
  triggersEnabled = false;
  await sql.task(async (conn) => {
    return await Promise.all(changes.map(change => change.revert(conn)));
  });
  setTimeout( () =>  { triggersEnabled = true }, 2000);
}


const stdin = process.stdin;
stdin.setRawMode(true);
stdin.resume();
stdin.setEncoding('utf8');
stdin.on('data', (key) => {
  if (key === 'r' || key === 'R') {
    console.log("All changes reverted!")
    revertAllChanges()
  } else if (key === '\u0003') {  // ctrl-c ( end of text )
    process.exit();
  }
});

function escapeKey(key, value) {
  if (Array.isArray(value)) {
    return `(\${${key}:csv})`;
  }
  return `\${${key}}`;
}

function createAndChain(row, appendedStr) {
  const conditions = [];
  for (const key of Object.keys(row)) {
    if (row[key] === null) {
      conditions.push(`${key} IS NULL`);
    } else {
      conditions.push(`${key} = ${escapeKey(key + appendedStr, row[key])}`);
    }
  }
  return conditions.join(' AND ');
}


function createSetChain(row) {
  const conditions = [];
  for (const key of Object.keys(row)) {
    conditions.push(`${key}= ${escapeKey(key, row[key])}`);
  }
  return conditions.join(', ');
}

function createValueChain(row) {
  const conditions = [];
  for (const key of Object.keys(row)) {
    conditions.push(escapeKey(key, row[key]));
  }
  return conditions.join(', ');
}

function appendToKeys(object, appendedStr) {
  const res = {};
  for (const key of Object.keys(object)) {
    res[`${key}${appendedStr}`] = object[key];
  }
  return res;
}

class UpdateChange {
  constructor(before, after, table) {
    this.before = before;
    this.after = after;
    this.table = table;
    this.changes = getChangedColumnsRow(before, after);
  }

  revert(conn) {
    const afterAppended = appendToKeys(this.after, suffix);
    const sqlCode = this.generateSQL();
    return conn.none(sqlCode, { ...afterAppended, ...this.before });
  }

  generateSQL() {
    return `
    UPDATE ${this.table} 
    SET ${createSetChain(this.changes)}
    WHERE ${createAndChain(this.before, suffix)}`;
  }
}

class InsertChange {
  constructor(after, table) {
    this.after = after;
    this.table = table;
  }

  revert(conn) {
    const sqlCode = this.generateSQL();
    return conn.none(sqlCode, { ...this.after });
  }

  generateSQL() {
    return `
    DELETE FROM ${this.table} 
    WHERE ${createAndChain(this.after, '')}`;
  }
}

class DeleteChange {
  constructor(before, table) {
    this.before = before;
    this.table = table;
  }

  revert(conn) {
    const sqlCode = this.generateSQL();
    return conn.none(sqlCode, { ...this.before });
  }

  generateSQL() {
    return `
    INSERT INTO ${this.table}
    VALUES (${createValueChain(this.before)})`;
  }
}

async function onUpdate({ table, before, after }) {
  if(!triggersEnabled) return;
  console.log(`UPDATE TABLE: ${table}`);
  console.log(getChanges(before, after, true));
  const change = new UpdateChange(before, after, table);
  addChange(change)
  // const afterAppended = appendToKeys(change.after, suffix);
  // const sqlCode = change.generateSQL();
  // console.log(Pg.as.format(sqlCode, { ...afterAppended, ...change.before }));
}

function onInsert({ table, after }) {
  if (!triggersEnabled) return;
  console.log(`INSERT TABLE: ${table}`);
  console.log(after);
  const change = new InsertChange(after, table);
  addChange(change)
  // change.revert(sql);
}

function onDelete({ table, before }) {
  if (!triggersEnabled) return;
  console.log(`DELETE TABLE: ${table}`);
  console.log(before);
  const change = new DeleteChange(before, table);
  addChange(change)
}

function getChanges(before, after, includeId = false) {
  const res = {};
  for (const key of Object.keys(before)) {
    if (before[key] !== after[key]) {
      res[key] = { before: before[key], after: after[key] };
    } else if (/id/.test(key) && includeId) {
      res[key] = before[key];
    }
  }
  return res;
}

function getChangedColumnsRow(before, after) {
  const changes = getChanges(before, after);
  const result = {};
  for (const key of Object.keys(changes)) {
    const { after } = changes[key];
    result[key] = after;
  }
  return result;
}

module.exports = {
  onUpdate,
  onDelete,
  onInsert,
};
