import { SEP } from './util.mjs'
import { KeyViolation } from './errors.mjs'

export default class Index {
  static create (options) {
    if (options.name === 'primary') options.unique = true
    const Factory = options.unique ? UniqueIndex : Index
    return new Factory(options)
  }

  constructor (options) {
    const { name, fields, unique } = options
    Object.assign(this, { name, fields, unique })
    this.function = row => fields.map(k => row[k]).join(SEP)
    this.data = new Map()
  }

  get options () {
    return { name: this.name, fields: this.fields, unique: !!this.unique }
  }

  locate (data) {
    if (typeof data !== 'object' && this.fields.length === 1) {
      return this.data.get(String(data))
    } else {
      return this.data.get(this.function(data))
    }
  }

  find (data) {
    const docs = this.locate(data)
    return docs ? Array.from(docs) : []
  }

  findOne (data) {
    const docs = this.locate(data)
    return docs ? docs.values().next().value : undefined
  }

  addDoc (doc) {
    const key = this.function(doc)
    const docs = this.data.get(key)
    if (docs) docs.add(doc)
    else this.data.set(key, new Set([doc]))
  }

  removeDoc (doc) {
    const key = this.function(doc)
    const docs = this.data.get(key)
    /* c8 ignore next */
    if (!docs) return
    docs.delete(doc)
    if (!docs.size) this.data.delete(key)
  }
}

class UniqueIndex extends Index {
  findOne (data) {
    return this.locate(data)
  }

  find (data) {
    return this.findOne(data)
  }

  addDoc (doc) {
    const key = this.function(doc)
    if (this.data.has(key)) throw new KeyViolation(doc, this.name)
    this.data.set(key, doc)
  }

  removeDoc (doc) {
    const key = this.function(doc)
    if (this.data.get(key) === doc) this.data.delete(key)
  }
}
