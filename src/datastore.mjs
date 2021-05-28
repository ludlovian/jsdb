import { readFile, appendFile, open, rename } from 'fs/promises'

import Serial from 'pixutil/serial'

import { NotExists, KeyViolation, NoIndex } from './errors.mjs'
import { getId, cleanObject, parse, stringify } from './util.mjs'
import Index from './dbindex.mjs'
import { lockFile } from './lockfile.mjs'

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

    const serial = new Serial()
    this._exec = serial.exec.bind(serial)
    this.loaded = false
    this.empty()
  }

  // API from Database class - mostly async

  exec (fn) {
    if (this.loaded) return this._exec(fn)
    this.loaded = true
    return this._exec(async () => {
      await lockFile(this.options.filename)
      await this.hydrate()
      await this.rewrite()
      return await fn()
    })
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

  find (fieldName, value) {
    if (!this.hasIndex(fieldName)) throw new NoIndex(fieldName)
    return this.indexes[fieldName].find(value)
  }

  findOne (fieldName, value) {
    if (!this.hasIndex(fieldName)) throw new NoIndex(fieldName)
    return this.indexes[fieldName].findOne(value)
  }

  allDocs () {
    return Array.from(this.indexes._id.data.values())
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

  async rewrite ({ sortBy } = {}) {
    const {
      filename,
      serialize,
      special: { addIndex }
    } = this.options
    const temp = filename + '~'
    const docs = this.allDocs()
    if (sortBy && typeof sortBy === 'function') docs.sort(sortBy)
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

  // Internal methods - mostly sync

  empty () {
    this.indexes = {
      _id: Index.create({ fieldName: '_id', unique: true })
    }
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

  addDoc (doc, { mustExist = false, mustNotExist = false } = {}) {
    const { _id, ...rest } = doc
    const olddoc = this.indexes._id.findOne(_id)
    if (!olddoc && mustExist) throw new NotExists(doc)
    if (olddoc && mustNotExist) throw new KeyViolation(doc, '_id')

    doc = {
      _id: _id || getId(doc, this.indexes._id.data),
      ...cleanObject(rest)
    }
    Object.freeze(doc)

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
    const olddoc = this.indexes._id.findOne(doc._id)
    if (!olddoc) throw new NotExists(doc)
    ixs.forEach(ix => ix.removeDoc(olddoc))
    return olddoc
  }
}
