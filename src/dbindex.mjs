import { KeyViolation } from './errors.mjs'

// Indexes are maps between values and docs
//
// Generic index is many-to-many
// Unique index is many values to single doc
// Sparse indexes do not index null-ish values
//
export default class Index {
  static create (options) {
    return new (options.unique ? UniqueIndex : Index)(options)
  }

  constructor (options) {
    this.options = options
    this.data = new Map()
  }

  find (value) {
    const docs = this.data.get(value)
    return docs ? Array.from(docs) : []
  }

  findOne (value) {
    const docs = this.data.get(value)
    return docs ? docs.values().next().value : undefined
  }

  addDoc (doc) {
    const value = doc[this.options.fieldName]
    if (Array.isArray(value)) {
      value.forEach(v => this.linkValueToDoc(v, doc))
    } else {
      this.linkValueToDoc(value, doc)
    }
  }

  removeDoc (doc) {
    const value = doc[this.options.fieldName]
    if (Array.isArray(value)) {
      value.forEach(v => this.unlinkValueFromDoc(v, doc))
    } else {
      this.unlinkValueFromDoc(value, doc)
    }
  }

  linkValueToDoc (value, doc) {
    if (value == null && this.options.sparse) return
    const docs = this.data.get(value)
    if (docs) {
      docs.add(doc)
    } else {
      this.data.set(value, new Set([doc]))
    }
  }

  unlinkValueFromDoc (value, doc) {
    const docs = this.data.get(value)
    if (!docs) return
    docs.delete(doc)
    if (!docs.size) this.data.delete(value)
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
    this.data.set(value, doc)
  }

  unlinkValueFromDoc (value, doc) {
    if (this.data.get(value) === doc) this.data.delete(value)
  }
}
