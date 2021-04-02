import Lock from 'plock';
import fs from 'fs';

class DatastoreError extends Error {
  constructor (name, message) {
    super(message);
    this.name = name;
    Error.captureStackTrace(this, this.constructor);
  }
}
class KeyViolation extends DatastoreError {
  constructor (doc, fieldName) {
    super('KeyViolation', 'Key violation error');
    this.fieldName = fieldName;
    this.record = doc;
  }
}
class NotExists extends DatastoreError {
  constructor (doc) {
    super('NotExists', 'Record does not exist');
    this.record = doc;
  }
}
class NoIndex extends DatastoreError {
  constructor (fieldName) {
    super('NoIndex', 'No such index');
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
const DATE_SENTINEL = '$date';
function stringify (obj) {
  return JSON.stringify(obj, function (k, v) {
    return this[k] instanceof Date
      ? { [DATE_SENTINEL]: this[k].toISOString() }
      : v
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
    selector = x => delve(x, key);
  }
  return (a, b) => {
    const x = selector(a);
    const y = selector(b);
    return x < y ? -1 : x > y ? 1 : 0
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
    const docs = this.data.get(value);
    return docs ? Array.from(docs) : []
  }
  findOne (value) {
    const docs = this.data.get(value);
    return docs ? docs.values().next().value : undefined
  }
  addDoc (doc) {
    const value = delve(doc, this.options.fieldName);
    if (Array.isArray(value)) {
      value.forEach(v => this.linkValueToDoc(v, doc));
    } else {
      this.linkValueToDoc(value, doc);
    }
  }
  removeDoc (doc) {
    const value = delve(doc, this.options.fieldName);
    if (Array.isArray(value)) {
      value.forEach(v => this.unlinkValueFromDoc(v, doc));
    } else {
      this.unlinkValueFromDoc(value, doc);
    }
  }
  linkValueToDoc (value, doc) {
    if (value == null && this.options.sparse) return
    const docs = this.data.get(value);
    if (docs) {
      docs.add(doc);
    } else {
      this.data.set(value, new Set([doc]));
    }
  }
  unlinkValueFromDoc (value, doc) {
    const docs = this.data.get(value);
    if (!docs) return
    docs.delete(doc);
    if (!docs.size) this.data.delete(value);
  }
}
class UniqueIndex extends Index {
  findOne (value) {
    return this.data.get(value)
  }
  find (value) {
    return this.findOne(value)
  }
  linkValueToDoc (value, doc) {
    if (value == null && this.options.sparse) return
    if (this.data.has(value)) {
      throw new KeyViolation(doc, this.options.fieldName)
    }
    this.data.set(value, doc);
  }
  unlinkValueFromDoc (value, doc) {
    if (this.data.get(value) === doc) this.data.delete(value);
  }
}

const { readFile, appendFile, open, rename } = fs.promises;
class Datastore {
  constructor (options) {
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
    this.empty();
  }
  empty () {
    this.indexes = {
      _id: Index.create({ fieldName: '_id', unique: true })
    };
  }
  async ensureIndex (options) {
    const { fieldName } = options;
    const { addIndex } = this.options.special;
    if (this.hasIndex(fieldName)) return
    this.addIndex(options);
    await this.append([{ [addIndex]: options }]);
  }
  async deleteIndex (fieldName) {
    const { deleteIndex } = this.options.special;
    if (fieldName === '_id') return
    if (!this.hasIndex(fieldName)) throw new NoIndex(fieldName)
    this.removeIndex(fieldName);
    await this.append([{ [deleteIndex]: { fieldName } }]);
  }
  addIndex (options) {
    const { fieldName } = options;
    const ix = Index.create(options);
    this.allDocs().forEach(doc => ix.addDoc(doc));
    this.indexes[fieldName] = ix;
  }
  removeIndex (fieldName) {
    delete this.indexes[fieldName];
  }
  hasIndex (fieldName) {
    return Boolean(this.indexes[fieldName])
  }
  find (fieldName, value) {
    if (!this.hasIndex(fieldName)) throw new NoIndex(fieldName)
    return this.indexes[fieldName].find(value)
  }
  findOne (fieldName, value) {
    if (!this.hasIndex(fieldName)) throw new NoIndex(fieldName)
    return this.indexes[fieldName].findOne(value)
  }
  async upsert (docOrDocs, options) {
    let ret;
    let docs;
    if (Array.isArray(docOrDocs)) {
      ret = docOrDocs.map(doc => this.addDoc(doc, options));
      docs = ret;
    } else {
      ret = this.addDoc(docOrDocs, options);
      docs = [ret];
    }
    await this.append(docs);
    return ret
  }
  async delete (docOrDocs) {
    let ret;
    let docs;
    const { deleted } = this.options.special;
    if (Array.isArray(docOrDocs)) {
      ret = docOrDocs.map(doc => this.removeDoc(doc));
      docs = ret;
    } else {
      ret = this.removeDoc(docOrDocs);
      docs = [ret];
    }
    docs = docs.map(doc => ({ [deleted]: doc }));
    await this.append(docs);
    return ret
  }
  addDoc (doc, { mustExist = false, mustNotExist = false } = {}) {
    const { _id, ...rest } = doc;
    const olddoc = this.indexes._id.findOne(_id);
    if (!olddoc && mustExist) throw new NotExists(doc)
    if (olddoc && mustNotExist) throw new KeyViolation(doc, '_id')
    doc = {
      _id: _id || getId(doc, this.indexes._id.data),
      ...cleanObject(rest)
    };
    Object.freeze(doc);
    const ixs = Object.values(this.indexes);
    try {
      ixs.forEach(ix => {
        if (olddoc) ix.removeDoc(olddoc);
        ix.addDoc(doc);
      });
      return doc
    } catch (err) {
      ixs.forEach(ix => {
        ix.removeDoc(doc);
        if (olddoc) {
          ix.removeDoc(olddoc);
          ix.addDoc(olddoc);
        }
      });
      throw err
    }
  }
  removeDoc (doc) {
    const ixs = Object.values(this.indexes);
    const olddoc = this.indexes._id.findOne(doc._id);
    if (!olddoc) throw new NotExists(doc)
    ixs.forEach(ix => ix.removeDoc(olddoc));
    return olddoc
  }
  allDocs () {
    return Array.from(this.indexes._id.data.values())
  }
  async hydrate () {
    const {
      filename,
      deserialize,
      special: { deleted, addIndex, deleteIndex }
    } = this.options;
    const data = await readFile(filename, { encoding: 'utf8', flag: 'a+' });
    this.empty();
    for (const line of data.split(/\n/).filter(Boolean)) {
      const doc = deserialize(line);
      if (addIndex in doc) {
        this.addIndex(doc[addIndex]);
      } else if (deleteIndex in doc) {
        this.deleteIndex(doc[deleteIndex].fieldName);
      } else if (deleted in doc) {
        this.removeDoc(doc[deleted]);
      } else {
        this.addDoc(doc);
      }
    }
  }
  async rewrite ({ sorted = false } = {}) {
    const {
      filename,
      serialize,
      special: { addIndex }
    } = this.options;
    const temp = filename + '~';
    const docs = this.allDocs();
    if (sorted) {
      if (typeof sorted !== 'string' && typeof sorted !== 'function') {
        sorted = '_id';
      }
      docs.sort(sortOn(sorted));
    }
    const lines = Object.values(this.indexes)
      .filter(ix => ix.options.fieldName !== '_id')
      .map(ix => ({ [addIndex]: ix.options }))
      .concat(docs)
      .map(doc => serialize(doc) + '\n');
    const fh = await open(temp, 'w');
    await fh.writeFile(lines.join(''), 'utf8');
    await fh.sync();
    await fh.close();
    await rename(temp, filename);
  }
  async append (docs) {
    const { filename, serialize } = this.options;
    const lines = docs.map(doc => serialize(doc) + '\n').join('');
    await appendFile(filename, lines, 'utf8');
  }
}

class Database {
  constructor (options) {
    if (typeof options === 'string') options = { filename: options };
    if (!options) throw new TypeError('No options given')
    this.loaded = false;
    const lock = new Lock();
    Object.defineProperties(this, {
      _ds: {
        value: new Datastore(options),
        configurable: true
      },
      _lock: {
        value: lock,
        configurable: true
      },
      _execute: {
        value: lock.exec.bind(lock),
        configurable: true
      },
      _autoCompaction: {
        value: undefined,
        configurable: true,
        writable: true
      }
    });
    this._lock.acquire();
    if (options.autoload) this.load();
    if (options.autocompact) this.setAutoCompaction(options.autocompact);
  }
  async load () {
    if (this.loaded) return
    this.loaded = true;
    await this._ds.hydrate();
    await this._ds.rewrite();
    this._lock.release();
  }
  reload () {
    return this._execute(() => this._ds.hydrate())
  }
  compact (opts) {
    return this._execute(() => this._ds.rewrite(opts))
  }
  ensureIndex (options) {
    return this._execute(() => this._ds.ensureIndex(options))
  }
  deleteIndex (fieldName) {
    return this._execute(() => this._ds.deleteIndex(fieldName))
  }
  insert (docOrDocs) {
    return this._execute(() =>
      this._ds.upsert(docOrDocs, { mustNotExist: true })
    )
  }
  update (docOrDocs) {
    return this._execute(() => this._ds.upsert(docOrDocs, { mustExist: true }))
  }
  upsert (docOrDocs) {
    return this._execute(() => this._ds.upsert(docOrDocs))
  }
  delete (docOrDocs) {
    return this._execute(() => this._ds.delete(docOrDocs))
  }
  getAll () {
    return this._execute(async () => this._ds.allDocs())
  }
  find (fieldName, value) {
    return this._execute(async () => this._ds.find(fieldName, value))
  }
  findOne (fieldName, value) {
    return this._execute(async () => this._ds.findOne(fieldName, value))
  }
  setAutoCompaction (interval, opts) {
    this.stopAutoCompaction();
    this._autoCompaction = setInterval(() => this.compact(opts), interval);
  }
  stopAutoCompaction () {
    if (!this._autoCompaction) return
    clearInterval(this._autoCompaction);
    this._autoCompaction = undefined;
  }
}
Object.assign(Database, { KeyViolation, NotExists, NoIndex });

export default Database;
