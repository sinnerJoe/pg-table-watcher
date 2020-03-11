
const { green, red, bright, magenta } = require('ansicolor');
const Table = require('table-layout');
const { usedTriggers } = require('./constants'); 
const { sql, dropTrigger } = require('./database');

const tableConfig = {
  ignoreEmptyColumns: false,
  maxWidth: 128,
};

const ROW_SIZE = 12;


const suffix = '_after_tbw';

let triggersEnabled = true;

let changes = [];

function addChange(change) {
  changes.push(change);
}

async function revertAllChanges() {
  triggersEnabled = false;
  await sql.task(async conn => await Promise.all(changes.reverse().map(change => change.revert(conn))));
  await sql.any('COMMIT;');

  return new Promise(resolve => setTimeout(() => {
    triggersEnabled = true;
    resolve();
    changes = [];
  }, 2000));
}


const stdin = process.stdin;
stdin.setRawMode(true);
stdin.resume();
stdin.setEncoding('utf8');
stdin.on('data', async (key) => {
  if (key === 'r' || key === 'R') {
    if (changes.length === 0) {
      console.log('There are no registered changes to the DB!');
      return;
    }
    console.log(`Reverting ${changes.length} changes. The change listener is temporarily disabled.`);
    await revertAllChanges();
    console.log('All changes reverted!');
  } else if (key === '\u0003') {  // ctrl-c ( end of text )
    // for(const key of Object.keys(usedTriggers)) {
    //   await dropTrigger(key, )
    // }
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
    WHERE ${createAndChain(this.after, suffix)}`;
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

function getLabels(obj) {
  const res = {};
  for (const key of Object.keys(obj)) {
    res[key] = bright(key);
  }
  return res;
}

function surroundStringByQuotes(value) {
  if (typeof value === 'string') {
    return `\`${value}\``;
  }
  return value;
}

function outputUpdatedRow(rowChanges) {
  const before = {};
  const after = {};
  for (const key of Object.keys(rowChanges)) {
    if (typeof rowChanges[key] === 'object'
        && rowChanges[key] != null
        && Object.hasOwnProperty.call(rowChanges[key], 'before')) {
      before[key] = bright(red(surroundStringByQuotes(rowChanges[key].before)));
      after[key] = bright(green(surroundStringByQuotes(rowChanges[key].after)));
    } else {
      before[key] = rowChanges[key];
    }
  }
  const table = new Table([getLabels(before), before, after], tableConfig);
  console.log(table.toString());
}

function breakRow(row, columns) {
  const rows = [];
  const allLabels = Object.keys(row);
  for (let i = 0; i < allLabels.length; i += columns) {
    const littleRow = {};
    allLabels.slice(i, i + columns).forEach((key) => { littleRow[key] = row[key]; });
    rows.push(littleRow);
  }
  return rows;
}

function outputRow(row, color) {
  const outputObj = {};
  for (const key of Object.keys(row)) {
    outputObj[key] = bright(color(surroundStringByQuotes(row[key])));
  }
  const table = new Table([getLabels(outputObj), outputObj]);
  console.log(table.toString());
}

function isAnyDiff(diffs) {
  return Object.keys(diffs).some(
    (key) => (diffs[key] && typeof diffs[key] === 'object' && Object.hasOwnProperty.call(diffs[key], 'before')));
}

async function onUpdate({ table, before, after }) {
  if (!triggersEnabled) return;
  console.log(bright(magenta(`UPDATED TABLE: ${table}`)));
  const diffs = getChanges(before, after, true);
  breakRow(diffs, ROW_SIZE).forEach(outputUpdatedRow);
  if(!isAnyDiff(diffs)) {
    console.log("There are no differences in updated data")
    return;
  }
  const change = new UpdateChange(before, after, table);
  addChange(change);
}

function onInsert({ table, after }) {
  if (!triggersEnabled) return;
  console.log(magenta(`INSERT TABLE: ${table}`));
  breakRow(after, ROW_SIZE).forEach(row => outputRow(row, green));
  const change = new InsertChange(after, table);
  addChange(change);
}

function onDelete({ table, before }) {
  if (!triggersEnabled) return;
  console.log(magenta(`DELETE TABLE: ${table}`));
  breakRow(before, ROW_SIZE).forEach(row => outputRow(row, green));
  const change = new DeleteChange(before, table);
  addChange(change);
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
