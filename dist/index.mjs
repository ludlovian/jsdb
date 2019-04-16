import fs from 'fs';
import { promisify } from 'util';
import PLock from 'plock';

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

function delve (obj, key) {
  let p = 0;
  key = key.split('.');
  while (obj && p < key.length) {
    obj = obj[key[p++]];
  }
  return obj === undefined || p < key.length ? undefined : obj
}
function getRandomString (n) {
  return Math.random()
    .toString(36)
    .slice(2, 2 + n)
}
function getRandomId () {
  return `${getRandomString(6)}-${getRandomString(6)}`
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

class Index {
  static create (datastore, options) {
    return options.unique
      ? new UniqueIndex(datastore, options)
      : new Index(datastore, options)
  }
  constructor (datastore, options) {
    this._execute = datastore._execute.bind(datastore);
    this.options = options;
    this._data = new Map();
  }
  find (value) {
    return this._execute(() => this._data.get(value) || [])
  }
  findOne (value) {
    return this._execute(() => {
      const list = this._data.get(value);
      return list ? list[0] : undefined
    })
  }
  getAll () {
    return this._execute(() => Array.from(this._data.entries()))
  }
  _addLink (key, doc) {
    let list = this._data.get(key);
    if (!list) {
      list = [];
      this._data.set(key, list);
    }
    if (!list.includes(doc)) list.push(doc);
  }
  _removeLink (key, doc) {
    const list = this._data.get(key) || [];
    const index = list.indexOf(doc);
    if (index === -1) return
    list.splice(index, 1);
    if (!list.length) this._data.delete(key);
  }
  _insertDoc (doc) {
    let key = delve(doc, this.options.fieldName);
    if (key == null && this.options.sparse) return
    if (Array.isArray(key)) {
      key.forEach(key => this._addLink(key, doc));
    } else {
      this._addLink(key, doc);
    }
  }
  _deleteDoc (doc) {
    let key = delve(doc, this.options.fieldName);
    if (Array.isArray(key)) {
      key.forEach(key => this._removeLink(key, doc));
    } else {
      this._removeLink(key, doc);
    }
  }
}
class UniqueIndex extends Index {
  find (value) {
    return this.findOne(value)
  }
  findOne (value) {
    return this._execute(() => this._data.get(value))
  }
  _addLink (key, doc) {
    if (this._data.has(key)) {
      throw new KeyViolation(doc, this.options.fieldName)
    }
    this._data.set(key, doc);
  }
  _removeLink (key, doc) {
    this._data.delete(key);
  }
}

const readFile = promisify(fs.readFile);
const appendFile = promisify(fs.appendFile);
const openFile = promisify(fs.open);
const writeFile = promisify(fs.writeFile);
const syncFile = promisify(fs.fsync);
const closeFile = promisify(fs.close);
const renameFile = promisify(fs.rename);
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
    this._lock = new PLock();
    this._lock.lock();
    this._empty();
    if (options.autoload) this.load();
    if (options.autocompact) this.setAutoCompaction(options.autocompact);
  }
  async load () {
    if (this._loaded) return this._loaded
    this._loaded = this._hydrate()
      .then(() => this._lock.release())
      .then(() => this.compact())
      .then(() => {
        this.loaded = true;
      });
    return this._loaded
  }
  compact () {
    return this._execute(() => this._rewrite())
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
    if (this.indexes[fieldName]) return
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
  setAutoCompaction (interval) {
    this.stopAutoCompaction();
    this.autoCompaction = setInterval(() => this.compact(), interval);
  }
  stopAutoCompaction () {
    if (!this.autoCompaction) return
    clearInterval(this.autoCompaction);
    this.autoCompaction = undefined;
  }
  _execute (fn) {
    return this._lock.exec(fn)
  }
  _empty () {
    this.indexes = {
      _id: Index.create(this, { fieldName: '_id', unique: true })
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
      let doc = deserialize(line);
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
    return Array.from(this.indexes._id._data.values())
  }
  _addIndex (options) {
    const { fieldName } = options;
    const ix = Index.create(this, options);
    this._getAll().forEach(doc => ix._insertDoc(doc));
    this.indexes[fieldName] = ix;
  }
  _deleteIndex (fieldName) {
    delete this.indexes[fieldName];
  }
  _upsertDoc (doc, { mustExist = false, mustNotExist = false } = {}) {
    const olddoc = this.indexes._id._data.get(doc._id);
    if (!olddoc && mustExist) throw new NotExists(doc)
    if (olddoc && mustNotExist) throw new KeyViolation(doc, '_id')
    doc = cleanObject(doc);
    if (doc._id == null) doc = { _id: getRandomId(), ...doc };
    const ixs = Object.values(this.indexes);
    try {
      ixs.forEach(ix => {
        if (olddoc) ix._deleteDoc(olddoc);
        ix._insertDoc(doc);
      });
      return doc
    } catch (err) {
      ixs.forEach(ix => {
        ix._deleteDoc(doc);
        if (olddoc) {
          ix._deleteDoc(olddoc);
          ix._insertDoc(olddoc);
        }
      });
      throw err
    }
  }
  _deleteDoc (doc) {
    const ixs = Object.values(this.indexes);
    const olddoc = this.indexes._id._data.get(doc._id);
    if (!olddoc) throw new NotExists(doc)
    ixs.forEach(ix => ix._deleteDoc(olddoc));
    return olddoc
  }
  async _append (doc) {
    const { filename, serialize } = this.options;
    const line = serialize(doc) + '\n';
    await appendFile(filename, line, 'utf8');
  }
  async _rewrite (doc) {
    const {
      filename,
      serialize,
      special: { addIndex }
    } = this.options;
    const temp = filename + '~';
    const lines = Array.from(this._getAll()).map(doc => serialize(doc) + '\n');
    const indexes = Object.values(this.indexes)
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

Object.assign(Datastore, { KeyViolation, NotExists });

export default Datastore;
