import { homedir } from 'os'
import { resolve, join } from 'path'

import { NotExists, KeyViolation, NoIndex, DatabaseLocked } from './errors.mjs'
import Datastore from './datastore.mjs'

// Database
//
// The public API of a jsdb database
//
export default class Database {
  constructor (filename) {
    if (!filename || typeof filename !== 'string') {
      throw new TypeError('Bad filename')
    }
    filename = resolve(join(homedir(), '.databases'), filename)
    const ds = new Datastore(filename)
    Object.defineProperties(this, {
      _ds: { value: ds, configurable: true },
      _autoCompaction: { configurable: true, writable: true }
    })
  }

  load () {
    return this.reload()
  }

  reload () {
    return this._ds.exec(() => this._ds.hydrate())
  }

  compact (opts) {
    return this._ds.exec(() => this._ds.rewrite(opts))
  }

  ensureIndex (options) {
    return this._ds.exec(() => this._ds.ensureIndex(options))
  }

  deleteIndex (fieldName) {
    return this._ds.exec(() => this._ds.deleteIndex(fieldName))
  }

  insert (docOrDocs) {
    return this._ds.exec(() =>
      this._ds.upsert(docOrDocs, { mustNotExist: true })
    )
  }

  update (docOrDocs) {
    return this._ds.exec(() => this._ds.upsert(docOrDocs, { mustExist: true }))
  }

  upsert (docOrDocs) {
    return this._ds.exec(() => this._ds.upsert(docOrDocs))
  }

  delete (docOrDocs) {
    return this._ds.exec(() => this._ds.delete(docOrDocs))
  }

  getAll () {
    return this._ds.exec(async () => this._ds.allDocs())
  }

  find (fieldName, value) {
    return this._ds.exec(async () => this._ds.find(fieldName, value))
  }

  findOne (fieldName, value) {
    return this._ds.exec(async () => this._ds.findOne(fieldName, value))
  }

  setAutoCompaction (interval, opts) {
    this.stopAutoCompaction()
    this._autoCompaction = setInterval(() => this.compact(opts), interval)
  }

  stopAutoCompaction () {
    if (!this._autoCompaction) return
    clearInterval(this._autoCompaction)
    this._autoCompaction = undefined
  }
}

Object.assign(Database, { KeyViolation, NotExists, NoIndex, DatabaseLocked })
