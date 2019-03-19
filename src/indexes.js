'use strict'

import { KeyViolation } from './errors'
import { delve } from './util'

export default class Index {
  static create (options) {
    return options.unique ? new UniqueIndex(options) : new Index(options)
  }

  constructor (options) {
    this.options = options
    this._data = new Map()
  }

  find (value) {
    const list = this._data.get(value)
    return Promise.resolve(list || [])
  }

  findOne (value) {
    const list = this._data.get(value)
    return Promise.resolve(list ? list[0] : undefined)
  }

  getAll () {
    return Promise.resolve(Array.from(this._data.entries()))
  }

  // INTERNAL API
  _addLink (key, doc) {
    let list = this._data.get(key)
    if (!list) {
      list = []
      this._data.set(key, list)
    }
    if (list.indexOf(doc) === -1) list.push(doc)
  }

  _removeLink (key, doc) {
    const list = this._data.get(key) || []
    const index = list.indexOf(doc)
    if (index === -1) return
    list.splice(index, 1)
    if (!list.length) this._data.delete(key)
  }

  _insertDoc (doc) {
    let key = delve(doc, this.options.fieldName)
    if (key == null && this.options.sparse) return
    if (Array.isArray(key)) {
      key.forEach(key => this._addLink(key, doc))
    } else {
      this._addLink(key, doc)
    }
  }

  _deleteDoc (doc) {
    let key = delve(doc, this.options.fieldName)
    if (Array.isArray(key)) {
      key.forEach(key => this._removeLink(key, doc))
    } else {
      this._removeLink(key, doc)
    }
  }
}

class UniqueIndex extends Index {
  find (value) {
    return Promise.resolve(this._data.get(value))
  }

  findOne (value) {
    return Promise.resolve(this._data.get(value))
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
