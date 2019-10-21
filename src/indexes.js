'use strict'

import { KeyViolation } from './errors'
import { delve } from './util'

export default class Index {
  static create (datastore, options) {
    return options.unique
      ? new UniqueIndex(datastore, options)
      : new Index(datastore, options)
  }

  constructor (datastore, options) {
    this._execute = datastore._execute.bind(datastore)
    this.options = options
    this._data = new Map()
  }

  find (value) {
    return this._execute(() => this._data.get(value) || [])
  }

  findOne (value) {
    return this._execute(() => {
      const list = this._data.get(value)
      return list ? list[0] : undefined
    })
  }

  getAll () {
    return this._execute(() => Array.from(this._data.entries()))
  }

  // INTERNAL API
  _addLink (key, doc) {
    let list = this._data.get(key)
    if (!list) {
      list = []
      this._data.set(key, list)
    }
    if (!list.includes(doc)) list.push(doc)
  }

  _removeLink (key, doc) {
    const list = this._data.get(key) || []
    const index = list.indexOf(doc)
    if (index === -1) return
    list.splice(index, 1)
    if (!list.length) this._data.delete(key)
  }

  _insertDoc (doc) {
    const key = delve(doc, this.options.fieldName)
    if (key == null && this.options.sparse) return
    if (Array.isArray(key)) {
      key.forEach(key => this._addLink(key, doc))
    } else {
      this._addLink(key, doc)
    }
  }

  _deleteDoc (doc) {
    const key = delve(doc, this.options.fieldName)
    if (Array.isArray(key)) {
      key.forEach(key => this._removeLink(key, doc))
    } else {
      this._removeLink(key, doc)
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
    this._data.set(key, doc)
  }

  _removeLink (key, doc) {
    this._data.delete(key)
  }
}
