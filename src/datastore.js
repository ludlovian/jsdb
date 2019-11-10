'use strict'

import fs from 'fs'
import { promisify } from 'util'

import Index from './indexes'
import Queue from './queue'
import { NotExists, KeyViolation } from './errors'
import { getId, cleanObject, parse, stringify } from './util'

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
    this._empty()
    if (options.autoload) this.load()
    if (options.autocompact) this.setAutoCompaction(options.autocompact)
  }

  async load () {
    // if loading/loaded already, return the loaded promise
    if (this._loaded) return this._loaded

    this._loaded = this._hydrate()
      // start the queue
      .then(() => this._queue.start())
      // queue a compaction
      .then(() => this.compact())
      // everything now loaded
      .then(() => {
        this.loaded = true
      })

    return this._loaded
  }

  reload () {
    return this._execute(() => this._hydrate())
  }

  compact () {
    return this._execute(() => this._rewrite())
  }

  getAll () {
    return this._execute(() => this._getAll())
  }

  async insert (doc) {
    return this._execute(async () => {
      doc = await this._upsertDoc(doc, { mustNotExist: true })
      await this._append(doc)
      return doc
    })
  }

  async update (doc) {
    return this._execute(async () => {
      doc = await this._upsertDoc(doc, { mustExist: true })
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

  async ensureIndex (options) {
    const { fieldName } = options
    const { addIndex } = this.options.special
    if (this.indexes[fieldName]) return
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

  setAutoCompaction (interval) {
    this.stopAutoCompaction()
    this.autoCompaction = setInterval(() => this.compact(), interval)
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
    this.indexes = {
      _id: Index.create(this, { fieldName: '_id', unique: true })
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
    return Array.from(this.indexes._id._data.values())
  }

  _addIndex (options) {
    const { fieldName } = options
    const ix = Index.create(this, options)
    this._getAll().forEach(doc => ix._insertDoc(doc))
    this.indexes[fieldName] = ix
  }

  _deleteIndex (fieldName) {
    delete this.indexes[fieldName]
  }

  async _upsertDoc (doc, { mustExist = false, mustNotExist = false } = {}) {
    const olddoc = this.indexes._id._data.get(doc._id)
    if (!olddoc && mustExist) throw new NotExists(doc)
    if (olddoc && mustNotExist) throw new KeyViolation(doc, '_id')
    doc = cleanObject(doc)
    if (doc._id == null) {
      const _id = getId(doc, this.indexes._id._data)
      doc = { _id, ...doc }
    }
    const ixs = Object.values(this.indexes)
    try {
      ixs.forEach(ix => {
        if (olddoc) ix._deleteDoc(olddoc)
        ix._insertDoc(doc)
      })
      return doc
    } catch (err) {
      // rollback
      await this._hydrate()
      throw err
    }
  }

  _deleteDoc (doc) {
    const ixs = Object.values(this.indexes)
    const olddoc = this.indexes._id._data.get(doc._id)
    if (!olddoc) throw new NotExists(doc)
    ixs.forEach(ix => ix._deleteDoc(olddoc))
    return olddoc
  }

  async _append (doc) {
    const { filename, serialize } = this.options
    const line = serialize(doc) + '\n'
    await appendFile(filename, line, 'utf8')
  }

  async _rewrite (doc) {
    const {
      filename,
      serialize,
      special: { addIndex }
    } = this.options
    const temp = filename + '~'
    const lines = Array.from(this._getAll()).map(doc => serialize(doc) + '\n')
    const indexes = Object.values(this.indexes)
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
