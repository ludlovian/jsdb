import { homedir } from 'os'
import { resolve, join } from 'path'

import Lock from 'plock'

import { NotExists, KeyViolation, NoIndex } from './errors.mjs'
import Datastore from './datastore.mjs'
import { lockFile } from './lockfile.mjs'

// Database
//
// The public API of a jsdb database
//
export default class Database {
  constructor (options) {
    if (typeof options === 'string') options = { filename: options }
    if (!options) throw new TypeError('No options given')

    this.filename = resolve(join(homedir(), '.databases'), options.filename)
    this.loaded = false
    const lock = new Lock()

    Object.defineProperties(this, {
      _ds: {
        value: new Datastore({ ...options, filename: this.filename }),
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
    })

    this._lock.acquire()
    if (options.autoload) this.load()
    if (options.autocompact) this.setAutoCompaction(options.autocompact)
  }

  async load () {
    if (this.loaded) return
    this.loaded = true
    await lockFile(this.filename)
    await this._ds.hydrate()
    await this._ds.rewrite()
    this._lock.release()
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
    this.stopAutoCompaction()
    this._autoCompaction = setInterval(() => this.compact(opts), interval)
  }

  stopAutoCompaction () {
    if (!this._autoCompaction) return
    clearInterval(this._autoCompaction)
    this._autoCompaction = undefined
  }
}

Object.assign(Database, { KeyViolation, NotExists, NoIndex })
