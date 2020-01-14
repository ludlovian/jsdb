import fs from 'fs'

import { NotExists, KeyViolation, NoIndex } from './errors'
import { getId, cleanObject, parse, stringify, sortOn } from './util'
import Index from './dbindex'

const { readFile, appendFile, open, rename } = fs.promises

export default class Datastore {
  constructor (options) {
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

    this.empty()
  }

  empty () {
    this.indexes = {
      _id: Index.create({ fieldName: '_id', unique: true })
    }
  }

  async ensureIndex (options) {
    const { fieldName } = options
    const { addIndex } = this.options.special
    if (this.hasIndex(fieldName)) return
    this.addIndex(options)
    await this.append([{ [addIndex]: options }])
  }

  async deleteIndex (fieldName) {
    const { deleteIndex } = this.options.special
    if (fieldName === '_id') return
    if (!this.hasIndex(fieldName)) throw new NoIndex(fieldName)
    this.removeIndex(fieldName)
    await this.append([{ [deleteIndex]: { fieldName } }])
  }

  addIndex (options) {
    const { fieldName } = options
    const ix = Index.create(options)
    this.allDocs().forEach(doc => ix.addDoc(doc))
    this.indexes[fieldName] = ix
  }

  removeIndex (fieldName) {
    delete this.indexes[fieldName]
  }

  hasIndex (fieldName) {
    return Boolean(this.indexes[fieldName])
  }

  find (fieldName, value) {
    if (!this.hasIndex(fieldName)) throw new NoIndex(fieldName)
    return this.indexes[fieldName].find(value)
  }

  findOne (fieldName, value) {
    if (!this.hasIndex(fieldName)) throw new NoIndex(fieldName)
    return this.indexes[fieldName].findOne(value)
  }

  async upsert (docOrDocs, options) {
    let ret
    let docs
    if (Array.isArray(docOrDocs)) {
      ret = docOrDocs.map(doc => this.addDoc(doc, options))
      docs = ret
    } else {
      ret = this.addDoc(docOrDocs, options)
      docs = [ret]
    }
    await this.append(docs)
    return ret
  }

  async delete (docOrDocs) {
    let ret
    let docs
    const { deleted } = this.options.special
    if (Array.isArray(docOrDocs)) {
      ret = docOrDocs.map(doc => this.removeDoc(doc))
      docs = ret
    } else {
      ret = this.removeDoc(docOrDocs)
      docs = [ret]
    }
    docs = docs.map(doc => ({ [deleted]: doc }))
    await this.append(docs)
    return ret
  }

  addDoc (doc, { mustExist = false, mustNotExist = false } = {}) {
    const { _id, ...rest } = doc
    const olddoc = this.findOne('_id', _id)
    if (!olddoc && mustExist) throw new NotExists(doc)
    if (olddoc && mustNotExist) throw new KeyViolation(doc, '_id')

    doc = {
      _id: _id || getId(doc, this.indexes._id.data),
      ...cleanObject(rest)
    }

    const ixs = Object.values(this.indexes)
    try {
      ixs.forEach(ix => {
        if (olddoc) ix.removeDoc(olddoc)
        ix.addDoc(doc)
      })
      return doc
    } catch (err) {
      // to rollback, we remove the new doc from each index. If there is
      // an old one, then we remove that (just in case) and re-add
      ixs.forEach(ix => {
        ix.removeDoc(doc)
        if (olddoc) {
          ix.removeDoc(olddoc)
          ix.addDoc(olddoc)
        }
      })
      throw err
    }
  }

  removeDoc (doc) {
    const ixs = Object.values(this.indexes)
    const olddoc = this.findOne('_id', doc._id)
    if (!olddoc) throw new NotExists(doc)
    ixs.forEach(ix => ix.removeDoc(olddoc))
    return olddoc
  }

  allDocs () {
    return Array.from(this.indexes._id.data.values())
  }

  async hydrate () {
    const {
      filename,
      deserialize,
      special: { deleted, addIndex, deleteIndex }
    } = this.options

    const data = await readFile(filename, { encoding: 'utf8', flag: 'a+' })

    this.empty()
    for (const line of data.split(/\n/).filter(Boolean)) {
      const doc = deserialize(line)
      if (addIndex in doc) {
        this.addIndex(doc[addIndex])
      } else if (deleteIndex in doc) {
        this.deleteIndex(doc[deleteIndex].fieldName)
      } else if (deleted in doc) {
        this.removeDoc(doc[deleted])
      } else {
        this.addDoc(doc)
      }
    }
  }

  async rewrite ({ sorted = false } = {}) {
    const {
      filename,
      serialize,
      special: { addIndex }
    } = this.options
    const temp = filename + '~'
    const docs = this.allDocs()
    if (sorted) {
      if (typeof sorted !== 'string' && typeof sorted !== 'function') {
        sorted = '_id'
      }
      docs.sort(sortOn(sorted))
    }
    const lines = Object.values(this.indexes)
      .filter(ix => ix.options.fieldName !== '_id')
      .map(ix => ({ [addIndex]: ix.options }))
      .concat(docs)
      .map(doc => serialize(doc) + '\n')
    const fh = await open(temp, 'w')
    await fh.writeFile(lines.join(''), 'utf8')
    await fh.sync()
    await fh.close()
    await rename(temp, filename)
  }

  async append (docs) {
    const { filename, serialize } = this.options
    const lines = docs.map(doc => serialize(doc) + '\n').join('')
    await appendFile(filename, lines, 'utf8')
  }
}
