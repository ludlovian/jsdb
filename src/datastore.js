'use strict'

import fs from 'fs'
import { promisify } from 'util'

import Queue from './queue'
import { NotExists, KeyViolation, NoIndex } from './errors'
import { getId, cleanObject, parse, stringify, delve, sortOn } from './util'

const readFile = promisify(fs.readFile)
const appendFile = promisify(fs.appendFile)
const openFile = promisify(fs.open)
const writeFile = promisify(fs.writeFile)
const syncFile = promisify(fs.fsync)
const closeFile = promisify(fs.close)
const renameFile = promisify(fs.rename)

export default class Datastore {
  constructor (options) {
    if (typeof options === 'string') options = { filename: options }
    this.options = {
      serialize: stringify,
      deserialize: parse,
      special: {
        deleted: '$$deleted',
        addIndex: '$$addIndex',
        deleteIndex: '$$deleteIndex'
      },
      ...options
    }
    this.loaded = false
    this._queue = new Queue()
    this._queue.add(
      () =>
        new Promise(resolve => {
          this._starter = resolve
        })
    )
    this._empty()
    if (options.autoload) this.load()
    if (options.autocompact) this.setAutoCompaction(options.autocompact)
  }

  // Loading and compaction

  async load () {
    if (this.loaded || this.loading) return
    this.loading = true

    await this._hydrate()
    await this._rewrite()

    this.loaded = true
    this.loading = false

    // start the queue
    this._starter()
  }

  reload () {
    return this._execute(() => this._hydrate())
  }

  compact (opts) {
    return this._execute(() => this._rewrite(opts))
  }

  // Data modification

  getAll () {
    return this._execute(() => this._getAll())
  }

  async insert (doc) {
    return this._execute(async () => {
      doc = this._upsertDoc(doc, { mustNotExist: true })
      await this._append(doc)
      return doc
    })
  }

  async update (doc) {
    return this._execute(async () => {
      doc = this._upsertDoc(doc, { mustExist: true })
      await this._append(doc)
      return doc
    })
  }

  async upsert (doc) {
    return this._execute(async () => {
      doc = this._upsertDoc(doc)
      await this._append(doc)
      return doc
    })
  }

  async delete (doc) {
    const { deleted } = this.options.special
    return this._execute(async () => {
      doc = this._deleteDoc(doc)
      await this._append({ [deleted]: doc })
      return doc
    })
  }

  // Indexes

  async ensureIndex (options) {
    const { fieldName } = options
    const { addIndex } = this.options.special
    if (this._indexes[fieldName]) return
    return this._execute(() => {
      this._addIndex(options)
      return this._append({ [addIndex]: options })
    })
  }

  async deleteIndex (fieldName) {
    if (fieldName === '_id') return
    const { deleteIndex } = this.options.special
    return this._execute(() => {
      this._deleteIndex(fieldName)
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
    this.stopAutoCompaction()
    this.autoCompaction = setInterval(() => this.compact(opts), interval)
  }

  stopAutoCompaction () {
    if (!this.autoCompaction) return
    clearInterval(this.autoCompaction)
    this.autoCompaction = undefined
  }

  // PRIVATE API

  _execute (fn) {
    return this._queue.add(fn)
  }

  _empty () {
    this._indexes = {
      _id: Index.create({ fieldName: '_id', unique: true })
    }
  }

  async _hydrate () {
    const {
      filename,
      deserialize,
      special: { deleted, addIndex, deleteIndex }
    } = this.options

    const data = await readFile(filename, { encoding: 'utf8', flag: 'a+' })

    this._empty()
    for (const line of data.split(/\n/).filter(Boolean)) {
      const doc = deserialize(line)
      if (addIndex in doc) {
        this._addIndex(doc[addIndex])
      } else if (deleteIndex in doc) {
        this._deleteIndex(doc[deleteIndex].fieldName)
      } else if (deleted in doc) {
        this._deleteDoc(doc[deleted])
      } else {
        this._upsertDoc(doc)
      }
    }
  }

  _getAll () {
    return Array.from(this._indexes._id.data.values())
  }

  _addIndex (options) {
    const { fieldName } = options
    const ix = Index.create(options)
    this._getAll().forEach(doc => ix.insertDoc(doc))
    this._indexes[fieldName] = ix
  }

  _deleteIndex (fieldName) {
    delete this._indexes[fieldName]
  }

  _upsertDoc (doc, opts = {}) {
    if (Array.isArray(doc)) {
      return doc.map(d => this._upsertDoc(d, opts))
    }
    const { mustExist = false, mustNotExist = false } = opts
    const olddoc = this._indexes._id.find(doc._id)
    if (!olddoc && mustExist) throw new NotExists(doc)
    if (olddoc && mustNotExist) throw new KeyViolation(doc, '_id')
    doc = cleanObject(doc)
    if (doc._id == null) {
      const _id = getId(doc, this._indexes._id.data)
      doc = { _id, ...doc }
    }
    const ixs = Object.values(this._indexes)
    try {
      ixs.forEach(ix => {
        if (olddoc) ix.deleteDoc(olddoc)
        ix.insertDoc(doc)
      })
      return doc
    } catch (err) {
      ixs.forEach(ix => {
        ix.deleteDoc(doc)
        if (olddoc) {
          ix.deleteDoc(olddoc)
          ix.insertDoc(olddoc)
        }
      })
      throw err
    }
  }

  _deleteDoc (doc) {
    if (Array.isArray(doc)) {
      return doc.map(doc => this._deleteDoc(doc))
    }
    const ixs = Object.values(this._indexes)
    const olddoc = this._indexes._id.find(doc._id)
    if (!olddoc) throw new NotExists(doc)
    ixs.forEach(ix => ix.deleteDoc(olddoc))
    return olddoc
  }

  async _append (doc) {
    const { filename, serialize } = this.options
    const docs = Array.isArray(doc) ? doc : [doc]
    const lines = docs.map(d => serialize(d) + '\n').join('')
    await appendFile(filename, lines, 'utf8')
  }

  async _rewrite ({ sorted = false } = {}) {
    const {
      filename,
      serialize,
      special: { addIndex }
    } = this.options
    const temp = filename + '~'
    const docs = this._getAll()
    if (sorted) {
      if (typeof sorted !== 'string' && typeof sorted !== 'function') {
        sorted = '_id'
      }
      docs.sort(sortOn(sorted))
    }
    const lines = docs.map(doc => serialize(doc) + '\n')
    const indexes = Object.values(this._indexes)
      .filter(ix => ix.options.fieldName !== '_id')
      .map(ix => ({ [addIndex]: ix.options }))
      .map(doc => serialize(doc) + '\n')
    lines.push(...indexes)
    const fh = await openFile(temp, 'w')
    await writeFile(fh, lines.join(''), 'utf8')
    await syncFile(fh)
    await closeFile(fh)
    await renameFile(temp, filename)
  }
}

class Index {
  static create (options) {
    return new (options.unique ? UniqueIndex : Index)(options)
  }

  constructor (options) {
    this.options = options
    this.data = new Map()
  }

  find (value) {
    return this.data.get(value) || []
  }

  findOne (value) {
    const list = this.data.get(value)
    return list ? list[0] : undefined
  }

  findAll () {
    return Array.from(this.data.entries())
  }

  addLink (key, doc) {
    let list = this.data.get(key)
    if (!list) {
      list = []
      this.data.set(key, list)
    }
    if (!list.includes(doc)) list.push(doc)
  }

  removeLink (key, doc) {
    const list = this.data.get(key) || []
    const index = list.indexOf(doc)
    if (index === -1) return
    list.splice(index, 1)
    if (!list.length) this.data.delete(key)
  }

  insertDoc (doc) {
    const key = delve(doc, this.options.fieldName)
    if (key == null && this.options.sparse) return
    if (Array.isArray(key)) {
      key.forEach(key => this.addLink(key, doc))
    } else {
      this.addLink(key, doc)
    }
  }

  deleteDoc (doc) {
    const key = delve(doc, this.options.fieldName)
    if (Array.isArray(key)) {
      key.forEach(key => this.removeLink(key, doc))
    } else {
      this.removeLink(key, doc)
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
    this.data.set(key, doc)
  }

  removeLink (key, doc) {
    if (this.data.get(key) === doc) this.data.delete(key)
  }
}
