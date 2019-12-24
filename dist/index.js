'use strict';

function _interopDefault (ex) { return (ex && (typeof ex === 'object') && 'default' in ex) ? ex['default'] : ex; }

var fs = _interopDefault(require('fs'));
var util = require('util');

const resolved = Promise.resolve();
class Queue {
  constructor (width = 1) {
    let running = 0;
    const waiting = [];
    const listeners = [];
    Object.defineProperties(this, {
      running: {
        get: () => running
      },
      pending: {
        get: () => waiting.length
      }
    });
    this.add = fn =>
      new Promise((resolve, reject) => {
        const job = { fn, resolve, reject };
        if (running < width) startJob(job);
        else waiting.push(job);
      });
    this.wait = () =>
      !running ? resolved : new Promise(resolve => listeners.push(resolve));
    function startJob ({ fn, resolve, reject }) {
      running++;
      resolved
        .then(() => fn())
        .then(resolve, reject)
        .then(endJob);
    }
    function endJob () {
      if (--running < width && waiting.length) {
        startJob(waiting.shift());
      }
      if (running === 0) {
        listeners.splice(0).map(resolve => resolve());
      }
    }
  }
}

class DatastoreError extends Error {
  constructor (message) {
    super(message);
    this.name = this.constructor.name;
    Error.captureStackTrace(this, this.constructor);
  }
}
class KeyViolation extends DatastoreError {
  constructor (doc, fieldName) {
    super('Key violation error');
    this.fieldName = fieldName;
    this.record = doc;
  }
}
class NotExists extends DatastoreError {
  constructor (doc) {
    super('Record does not exist');
    this.record = doc;
  }
}
class NoIndex extends DatastoreError {
  constructor (fieldName) {
    super('No such index');
    this.fieldName = fieldName;
  }
}

function delve (obj, key) {
  let p = 0;
  key = key.split('.');
  while (obj && p < key.length) {
    obj = obj[key[p++]];
  }
  return obj === undefined || p < key.length ? undefined : obj
}
function getId (row, existing) {
  const start = hashString(stringify(row));
  for (let n = 0; n < 1e8; n++) {
    const id = ((start + n) & 0x7fffffff).toString(36);
    if (!existing.has(id)) return id
  }
  throw new Error('Could not generate unique id')
}
function hashString (string) {
  return Array.from(string).reduce(
    (h, ch) => ((h << 5) - h + ch.charCodeAt(0)) & 0xffffffff,
    0
  )
}
function cleanObject (obj) {
  return Object.entries(obj).reduce((o, [k, v]) => {
    if (v !== undefined) o[k] = v;
    return o
  }, {})
}
const DATE_SENTINEL = '$jsdb$date$';
function stringify (obj) {
  return JSON.stringify(obj, function (k, v) {
    return this[k] instanceof Date ? { [DATE_SENTINEL]: this[k].getTime() } : v
  })
}
function parse (s) {
  return JSON.parse(s, function (k, v) {
    if (k === DATE_SENTINEL) return new Date(v)
    if (typeof v === 'object' && DATE_SENTINEL in v) return v[DATE_SENTINEL]
    return v
  })
}
function sortOn (selector) {
  if (typeof selector !== 'function') {
    const key = selector;
    selector = x => x[key];
  }
  return (a, b) => {
    const x = selector(a);
    const y = selector(b);
    return x < y ? -1 : x > y ? 1 : 0
  }
}

const readFile = util.promisify(fs.readFile);
const appendFile = util.promisify(fs.appendFile);
const openFile = util.promisify(fs.open);
const writeFile = util.promisify(fs.writeFile);
const syncFile = util.promisify(fs.fsync);
const closeFile = util.promisify(fs.close);
const renameFile = util.promisify(fs.rename);
class Datastore {
  constructor (options) {
    if (typeof options === 'string') options = { filename: options };
    this.options = {
      serialize: stringify,
      deserialize: parse,
      special: {
        deleted: '$$deleted',
        addIndex: '$$addIndex',
        deleteIndex: '$$deleteIndex'
      },
      ...options
    };
    this.loaded = false;
    this._queue = new Queue();
    this._queue.add(
      () =>
        new Promise(resolve => {
          this._starter = resolve;
        })
    );
    this._empty();
    if (options.autoload) this.load();
    if (options.autocompact) this.setAutoCompaction(options.autocompact);
  }
  async load () {
    if (this.loaded || this.loading) return
    this.loading = true;
    await this._hydrate();
    await this._rewrite();
    this.loaded = true;
    this.loading = false;
    this._starter();
  }
  reload () {
    return this._execute(() => this._hydrate())
  }
  compact (opts) {
    return this._execute(() => this._rewrite(opts))
  }
  getAll () {
    return this._execute(() => this._getAll())
  }
  async insert (doc) {
    return this._execute(async () => {
      doc = this._upsertDoc(doc, { mustNotExist: true });
      await this._append(doc);
      return doc
    })
  }
  async update (doc) {
    return this._execute(async () => {
      doc = this._upsertDoc(doc, { mustExist: true });
      await this._append(doc);
      return doc
    })
  }
  async upsert (doc) {
    return this._execute(async () => {
      doc = this._upsertDoc(doc);
      await this._append(doc);
      return doc
    })
  }
  async delete (doc) {
    const { deleted } = this.options.special;
    return this._execute(async () => {
      doc = this._deleteDoc(doc);
      await this._append({ [deleted]: doc });
      return doc
    })
  }
  async ensureIndex (options) {
    const { fieldName } = options;
    const { addIndex } = this.options.special;
    if (this._indexes[fieldName]) return
    return this._execute(() => {
      this._addIndex(options);
      return this._append({ [addIndex]: options })
    })
  }
  async deleteIndex (fieldName) {
    if (fieldName === '_id') return
    const { deleteIndex } = this.options.special;
    return this._execute(() => {
      this._deleteIndex(fieldName);
      return this._append({ [deleteIndex]: { fieldName } })
    })
  }
  find (fieldName, value) {
    return this._execute(async () => {
      if (!this._indexes[fieldName]) throw new NoIndex(fieldName)
      return this._indexes[fieldName].find(value)
    })
  }
  findOne (fieldName, value) {
    return this._execute(async () => {
      if (!this._indexes[fieldName]) throw new NoIndex(fieldName)
      return this._indexes[fieldName].findOne(value)
    })
  }
  findAll (fieldName) {
    return this._execute(async () => {
      if (!this._indexes[fieldName]) throw new NoIndex(fieldName)
      return this._indexes[fieldName].findAll()
    })
  }
  setAutoCompaction (interval, opts) {
    this.stopAutoCompaction();
    this.autoCompaction = setInterval(() => this.compact(opts), interval);
  }
  stopAutoCompaction () {
    if (!this.autoCompaction) return
    clearInterval(this.autoCompaction);
    this.autoCompaction = undefined;
  }
  _execute (fn) {
    return this._queue.add(fn)
  }
  _empty () {
    this._indexes = {
      _id: Index.create({ fieldName: '_id', unique: true })
    };
  }
  async _hydrate () {
    const {
      filename,
      deserialize,
      special: { deleted, addIndex, deleteIndex }
    } = this.options;
    const data = await readFile(filename, { encoding: 'utf8', flag: 'a+' });
    this._empty();
    for (const line of data.split(/\n/).filter(Boolean)) {
      const doc = deserialize(line);
      if (addIndex in doc) {
        this._addIndex(doc[addIndex]);
      } else if (deleteIndex in doc) {
        this._deleteIndex(doc[deleteIndex].fieldName);
      } else if (deleted in doc) {
        this._deleteDoc(doc[deleted]);
      } else {
        this._upsertDoc(doc);
      }
    }
  }
  _getAll () {
    return Array.from(this._indexes._id.data.values())
  }
  _addIndex (options) {
    const { fieldName } = options;
    const ix = Index.create(options);
    this._getAll().forEach(doc => ix.insertDoc(doc));
    this._indexes[fieldName] = ix;
  }
  _deleteIndex (fieldName) {
    delete this._indexes[fieldName];
  }
  _upsertDoc (doc, opts = {}) {
    if (Array.isArray(doc)) {
      return doc.map(d => this._upsertDoc(d, opts))
    }
    const { mustExist = false, mustNotExist = false } = opts;
    const olddoc = this._indexes._id.find(doc._id);
    if (!olddoc && mustExist) throw new NotExists(doc)
    if (olddoc && mustNotExist) throw new KeyViolation(doc, '_id')
    doc = cleanObject(doc);
    if (doc._id == null) {
      const _id = getId(doc, this._indexes._id.data);
      doc = { _id, ...doc };
    }
    const ixs = Object.values(this._indexes);
    try {
      ixs.forEach(ix => {
        if (olddoc) ix.deleteDoc(olddoc);
        ix.insertDoc(doc);
      });
      return doc
    } catch (err) {
      ixs.forEach(ix => {
        ix.deleteDoc(doc);
        if (olddoc) {
          ix.deleteDoc(olddoc);
          ix.insertDoc(olddoc);
        }
      });
      throw err
    }
  }
  _deleteDoc (doc) {
    if (Array.isArray(doc)) {
      return doc.map(doc => this._deleteDoc(doc))
    }
    const ixs = Object.values(this._indexes);
    const olddoc = this._indexes._id.find(doc._id);
    if (!olddoc) throw new NotExists(doc)
    ixs.forEach(ix => ix.deleteDoc(olddoc));
    return olddoc
  }
  async _append (doc) {
    const { filename, serialize } = this.options;
    const docs = Array.isArray(doc) ? doc : [doc];
    const lines = docs.map(d => serialize(d) + '\n').join('');
    await appendFile(filename, lines, 'utf8');
  }
  async _rewrite ({ sorted = false } = {}) {
    const {
      filename,
      serialize,
      special: { addIndex }
    } = this.options;
    const temp = filename + '~';
    const docs = this._getAll();
    if (sorted) {
      if (typeof sorted !== 'string' && typeof sorted !== 'function') {
        sorted = '_id';
      }
      docs.sort(sortOn(sorted));
    }
    const lines = docs.map(doc => serialize(doc) + '\n');
    const indexes = Object.values(this._indexes)
      .filter(ix => ix.options.fieldName !== '_id')
      .map(ix => ({ [addIndex]: ix.options }))
      .map(doc => serialize(doc) + '\n');
    lines.push(...indexes);
    const fh = await openFile(temp, 'w');
    await writeFile(fh, lines.join(''), 'utf8');
    await syncFile(fh);
    await closeFile(fh);
    await renameFile(temp, filename);
  }
}
class Index {
  static create (options) {
    return new (options.unique ? UniqueIndex : Index)(options)
  }
  constructor (options) {
    this.options = options;
    this.data = new Map();
  }
  find (value) {
    return this.data.get(value) || []
  }
  findOne (value) {
    const list = this.data.get(value);
    return list ? list[0] : undefined
  }
  findAll () {
    return Array.from(this.data.entries())
  }
  addLink (key, doc) {
    let list = this.data.get(key);
    if (!list) {
      list = [];
      this.data.set(key, list);
    }
    if (!list.includes(doc)) list.push(doc);
  }
  removeLink (key, doc) {
    const list = this.data.get(key) || [];
    const index = list.indexOf(doc);
    if (index === -1) return
    list.splice(index, 1);
    if (!list.length) this.data.delete(key);
  }
  insertDoc (doc) {
    const key = delve(doc, this.options.fieldName);
    if (key == null && this.options.sparse) return
    if (Array.isArray(key)) {
      key.forEach(key => this.addLink(key, doc));
    } else {
      this.addLink(key, doc);
    }
  }
  deleteDoc (doc) {
    const key = delve(doc, this.options.fieldName);
    if (Array.isArray(key)) {
      key.forEach(key => this.removeLink(key, doc));
    } else {
      this.removeLink(key, doc);
    }
  }
}
class UniqueIndex extends Index {
  find (value) {
    return this.data.get(value)
  }
  findOne (value) {
    return this.find(value)
  }
  addLink (key, doc) {
    if (this.data.has(key)) {
      throw new KeyViolation(doc, this.options.fieldName)
    }
    this.data.set(key, doc);
  }
  removeLink (key, doc) {
    if (this.data.get(key) === doc) this.data.delete(key);
  }
}

Object.assign(Datastore, { KeyViolation, NotExists, NoIndex });

module.exports = Datastore;
