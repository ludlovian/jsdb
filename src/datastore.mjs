import { readFile, appendFile, open, rename } from 'fs/promises'

import Serial from 'pixutil/serial'
import arrify from 'pixutil/arrify'

import { NotExists, KeyViolation, NoIndex } from './errors.mjs'
import { getId, cleanObject, parse, stringify, SEP } from './util.mjs'
import Index from './dbindex.mjs'
import { lockFile } from './lockfile.mjs'

const ADD_INDEX = '$$addIndex'
const DELETE_INDEX = '$$deleteIndex'
const DELETED = '$$deleted'

export default class Datastore {
  constructor (filename) {
    this.filename = filename
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
      await lockFile(this.filename)
      await this.hydrate()
      await this.rewrite()
      return await fn()
    })
  }

  async ensureIndex (options) {
    if (options.field) {
      const { field, ...rest } = options
      options = { ...rest, fields: [field] }
    }
    const { name, fields } = options
    const existing = this.indexes[name]
    if (existing && existing.fields.join(SEP) === fields.join(SEP)) return
    const ix = this.addIndex(options)
    await this.append({ [ADD_INDEX]: ix.options })
  }

  async deleteIndex (name) {
    if (name === 'primary') return
    if (!this.indexes[name]) throw new NoIndex(name)
    this.removeIndex(name)
    await this.append([{ [DELETE_INDEX]: name }])
  }

  find (name, data) {
    if (name === '_id' && !this.indexes._id) name = 'primary'
    if (!this.indexes[name]) throw new NoIndex(name)
    return this.indexes[name].find(data)
  }

  findOne (name, data) {
    if (name === '_id' && !this.indexes._id) name = 'primary'
    if (!this.indexes[name]) throw new NoIndex(name)
    return this.indexes[name].findOne(data)
  }

  allDocs () {
    return [...this.indexes.primary.data.values()]
  }

  async upsert (doc, options) {
    doc = this.addDoc(doc, options)
    await this.append(doc)
    return doc
  }

  async delete (doc) {
    doc = this.removeDoc(doc)
    await this.append(arrify(doc).map(doc => ({ [DELETED]: doc })))
    return doc
  }

  async hydrate () {
    const data = await readFile(this.filename, { encoding: 'utf8', flag: 'a+' })

    this.empty()
    for (const line of data.split(/\n/).filter(Boolean)) {
      const doc = parse(line)
      if (ADD_INDEX in doc) {
        this.addIndex(doc[ADD_INDEX])
      } else if (DELETE_INDEX in doc) {
        this.deleteIndex(doc[DELETE_INDEX])
      } else if (DELETED in doc) {
        this.removeDoc(doc[DELETED])
      } else {
        this.addDoc(doc)
      }
    }
  }

  async rewrite ({ sortBy } = {}) {
    const temp = this.filename + '~'
    const docs = this.allDocs()
    if (sortBy && typeof sortBy === 'function') docs.sort(sortBy)
    const lines = Object.values(this.indexes)
      .map(ix => ({ [ADD_INDEX]: ix.options }))
      .concat(docs)
      .map(doc => stringify(doc) + '\n')
    const fh = await open(temp, 'w')
    await fh.writeFile(lines.join(''), 'utf8')
    await fh.sync()
    await fh.close()
    await rename(temp, this.filename)
  }

  async append (docs) {
    docs = arrify(docs)
    const lines = docs.map(doc => stringify(doc) + '\n').join('')
    await appendFile(this.filename, lines, 'utf8')
  }

  // Internal methods - mostly sync

  empty () {
    this.indexes = {
      primary: Index.create({ name: 'primary', fields: ['_id'] })
    }
  }

  addIndex (options) {
    if (options.fieldName) {
      options.name = options.fieldName
      options.fields = [options.fieldName]
    }
    const { name } = options
    const ix = Index.create(options)
    this.allDocs().forEach(doc => ix.addDoc(doc))
    this.indexes[name] = ix
    return ix
  }

  removeIndex (name) {
    this.indexes[name] = undefined
  }

  addDoc (doc, options = {}) {
    if (Array.isArray(doc)) return doc.map(d => this.addDoc(d, options))
    const { mustExist = false, mustNotExist = false } = options
    const olddoc = this.indexes.primary.findOne(doc)
    if (!olddoc && mustExist) throw new NotExists(doc)
    if (olddoc && mustNotExist) throw new KeyViolation(doc, 'primary')

    if (this.indexes.primary.fields.length === 1) {
      const idField = this.indexes.primary.fields[0]
      if (idField in doc && doc[idField] == null) {
        doc[idField] = getId(doc, this.indexes.primary.data)
      }
    }

    doc = Object.freeze(cleanObject(doc))

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
    if (Array.isArray(doc)) return doc.map(d => this.removeDoc(d))
    const olddoc = this.indexes.primary.findOne(doc)
    if (!olddoc) throw new NotExists(doc)
    const ixs = Object.values(this.indexes)
    ixs.forEach(ix => ix.removeDoc(olddoc))
    return olddoc
  }
}
