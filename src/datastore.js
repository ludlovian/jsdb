'use strict'

import fs from 'fs'
import { promisify } from 'util'
import Index from './indexes'
import pqueue from 'pqueue'
import trigger from 'trigger'
import { NotExists } from './errors'
import { getRandomId } from './util'

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
      serialize: JSON.stringify,
      deserialize: JSON.parse,
      special: {
        deleted: '$$deleted',
        addIndex: '$$addIndex',
        deleteIndex: '$$deleteIndex'
      },
      ...options
    }
    this.indexes = {
      _id: Index.create({ fieldName: '_id', unique: true })
    }
    this._loaded = trigger()
    this._queue = pqueue()
    this._queue.push(() => this._loaded)
    this.loaded = false
    if (options.autoload) this.load()
  }

  async load () {
    // if loading/loaded already, return the loaded promise
    if (this.loaded) return this._loaded
    await this._hydrate()
    return this.compact()
  }

  compact () {
    return this._execute(() => this._rewrite())
  }

  getAll () {
    return this._execute(() => this._getAll())
  }

  async insert (doc) {
    if (doc._id == null) doc._id = getRandomId()
    return this._execute(() => {
      this._upsertDoc(doc)
      return this._append(doc)
    })
  }

  async update (doc) {
    return this._execute(async () => {
      doc = await this._upsertDoc(doc, { mustExist: true })
      return this._append(doc)
    })
  }

  async delete (doc) {
    const { deleted } = this.options.special
    return this._execute(async () => {
      doc = await this._deleteDoc(doc)
      return this._append({ [deleted]: doc })
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
    return this._queue.push(fn)
  }

  _hydrate () {
    this.loaded = true

    const {
      filename,
      deserialize,
      special: { deleted, addIndex, deleteIndex }
    } = this.options

    return (
      readFile(filename, { encoding: 'utf8', flag: 'a+' })
        .then(data => {
          for (const line of data.split(/\n/).filter(Boolean)) {
            let doc = deserialize(line)
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
        })
        // fire the loaded trigger, releasing any DB requests
        .then(() => this._loaded.fire())
    )
  }

  _getAll () {
    return Array.from(this.indexes._id._data.values())
  }

  _addIndex (options) {
    const { fieldName } = options
    const ix = Index.create(options)
    this._getAll().forEach(doc => ix._insertDoc(doc))
    this.indexes[fieldName] = ix
  }

  _deleteIndex (fieldName) {
    delete this.indexes[fieldName]
  }

  _upsertDoc (doc, { mustExist = false } = {}) {
    const ixs = Object.values(this.indexes)
    const olddoc = this.indexes._id._data.get(doc._id)
    if (!olddoc && mustExist) throw new NotExists(doc)
    try {
      ixs.forEach(ix => {
        if (olddoc) ix._deleteDoc(olddoc)
        ix._insertDoc(doc)
      })
      return doc
    } catch (err) {
      ixs.forEach(ix => {
        ix._deleteDoc(doc)
        if (olddoc) {
          ix._deleteDoc(olddoc)
          ix._insertDoc(olddoc)
        }
      })
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
