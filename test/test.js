'use strict'
import test from 'ava'
import Datastore from '../src'
import fs from 'fs'
import { promisify } from 'util'
import { exec as _exec } from 'child_process'

const exec = promisify(_exec)
const readFile = promisify(fs.readFile)

const DIR = './assets~'
const delay = ms => new Promise(resolve => setTimeout(resolve, ms))

test.before(async t => {
  await exec(`rm -rf ${DIR};mkdir ${DIR}`)
})

test.after(async t => {
  await exec(`rm -rf ${DIR}`)
})

test.beforeEach(t => {
  t.context.file = `${DIR}/test-${Math.random()
    .toString(36)
    .slice(2, 10)}.db`
})

test.afterEach(async t => {
  try {
    await promisify(fs.unlink)(t.context.file)
  } catch (e) {}
})

test('basic', async t => {
  const db = new Datastore({
    filename: t.context.file,
    autoload: true
  })
  await db.load()
  await db.insert({ _id: 1, foo: 'bar', ignoreThis: undefined })
  await db.ensureIndex({ fieldName: 'foo', sparse: true })
  const file = await readFile(t.context.file, 'utf8')
  t.snapshot(file)
})

test('delayed load', async t => {
  const db = new Datastore({
    filename: t.context.file
  })
  db.insert({ _id: 1, foo: 'bar', ignoreThis: undefined })
  db.ensureIndex({ fieldName: 'foo', sparse: true })
  db.load()
  await db.getAll()
  const file = await readFile(t.context.file, 'utf8')
  t.snapshot(file)
})

test('full activity', async t => {
  let db = new Datastore(t.context.file)
  const date = new Date(2018, 0, 19, 12, 34, 56)
  await db.load()
  await db.insert({ _id: 1, foo: 'bar', date })
  let r
  r = await db.indexes._id.find(1)
  t.is(r.foo, 'bar')
  r = await db.indexes._id.findOne(1)
  t.is(r.foo, 'bar')
  t.is(r.date, date)

  await db.ensureIndex({ fieldName: 'foo', sparse: true })
  await db.ensureIndex({ fieldName: 'foo', sparse: true })
  r = await db.indexes.foo.find('bar')
  t.is(r[0]._id, 1)

  r = await db.indexes.foo.findOne('bar')
  t.is(r._id, 1)

  await db.insert({ _id: 2, foo: 'bar' })
  r = await db.indexes.foo.find('bar')
  t.is(r.length, 2)

  await db.update({ _id: 1, bar: 'quux' })
  r = await db.indexes.foo.find('bar')
  t.is(r.length, 1)

  await db.delete({ _id: 1 })

  await db.deleteIndex('foo')
  await db.deleteIndex('_id')

  const file = await readFile(t.context.file, 'utf8')
  t.snapshot(file)

  db = new Datastore(t.context.file)
  await db.load()
  t.falsy(db.indexes.foo)

  await db.ensureIndex({ fieldName: 'foo', sparse: true })
  await db.compact()

  await db.insert({ noId: true })
  t.is((await db.getAll()).length, 2)
  t.is((await db.indexes._id.getAll()).length, 2)
})

test('generated id', async t => {
  const db = new Datastore({
    filename: t.context.file,
    autoload: true
  })
  await db.load()
  await db.insert({ foo: 'bar' })
  t.snapshot(await readFile(t.context.file, 'utf8'))

  await db.insert({ foo: 'bar' })
  t.snapshot(await readFile(t.context.file, 'utf8'))

  t.is((await db.getAll()).length, 2)
})

test('reload', async t => {
  const db = new Datastore(t.context.file)
  await db.load()
  await db.insert({ _id: 1, foo: 'bar' })

  t.is((await db.getAll()).length, 1)

  await promisify(fs.writeFile)(t.context.file, '')
  await db.reload()

  t.is((await db.getAll()).length, 0)
})

test('empty data', async t => {
  const db = new Datastore(t.context.file)
  await db.load()
  t.is(await db.indexes._id.findOne(1), undefined)
  t.is(await db.indexes._id.find(1), undefined)
  await db.ensureIndex({ fieldName: 'foo' })
  t.is(await db.indexes.foo.findOne(1), undefined)
  t.deepEqual(await db.indexes.foo.find(1), [])
})

test('array indexes', async t => {
  const db = new Datastore(t.context.file)
  await db.load()
  await db.ensureIndex({ fieldName: 'foo' })
  await db.insert({ _id: 1, foo: ['bar', 'baz'] })
  await db.insert({ _id: 2, foo: ['bar', 'bar'] })
  let r = await db.indexes.foo.find('bar')
  t.is(r.length, 2)
  r = await db.indexes.foo.find('baz')
  t.is(r.length, 1)

  await db.update({ _id: 1, foo: 'bar' })
  r = await db.indexes.foo.find('baz')
  t.is(r.length, 0)
})

test('errors', async t => {
  const db = new Datastore(t.context.file)
  await db.load()
  await t.throwsAsync(() => db.delete({ _id: 1 }))
  await t.throwsAsync(() => db.update({ _id: 1 }))

  await db.insert({ _id: 'foo', bar: 'baz' })
  await t.throwsAsync(() => db.insert({ _id: 'foo', bar: 'baz' }))

  await db.ensureIndex({ fieldName: 'foo', unique: true })
  await db.insert({ _id: 1, foo: 'bar' })
  await t.throwsAsync(() => db.insert({ _id: 2, foo: 'bar' }))
  t.deepEqual(await db.indexes.foo.find('bar'), { _id: 1, foo: 'bar' })

  await db.insert({ _id: 2, foo: 'baz' })
  await t.throwsAsync(() => db.update({ _id: 1, foo: 'baz' }))
})

test('auto compaction', async t => {
  const db = new Datastore({
    filename: t.context.file,
    autocompact: 500
  })
  await db.load()
  await db.insert({ _id: 1 })
  await db.update({ _id: 1, foo: 'bar' })
  await delay(750)
  db.stopAutoCompaction()

  const file = await readFile(t.context.file, 'utf8')
  t.snapshot(file)
})

test('sorted', async t => {
  const db = new Datastore(t.context.file)
  await db.load()
  await db.insert({ _id: 'foo' })
  await db.insert({ _id: 'bar' })
  await db.insert({ _id: 'baz' })

  let file = await readFile(t.context.file, 'utf8')
  t.snapshot(file)

  await db.compact({ sorted: true })
  file = await readFile(t.context.file, 'utf8')
  t.snapshot(file)
})

test('upsert', async t => {
  const db = new Datastore(t.context.file)
  await db.load()
  await db.upsert({_id: 'foo', bar: 'baz'})

  let file = await readFile(t.context.file, 'utf8')
  t.snapshot(file)

  await db.upsert({_id: 'foo', bar: 'quux'})

  file = await readFile(t.context.file, 'utf8')
  t.snapshot(file)
})
