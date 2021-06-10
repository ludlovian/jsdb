import { test } from 'uvu'
import * as assert from 'uvu/assert'
import snapshot from './helpers/snapshot.mjs'

import { readFileSync, writeFileSync } from 'fs'
import { execSync } from 'child_process'
import { resolve } from 'path'
import sortBy from 'sortby'

import Database from '../src/index.mjs'

const DIR = resolve('test/assets')
const delay = ms => new Promise(resolve => setTimeout(resolve, ms))

test.before(ctx => {
  execSync(`rm -rf ${DIR};mkdir ${DIR}`)
  ctx.dbnum = 1
})

test.after(() => {
  execSync(`rm -rf ${DIR}`)
})

test.before.each(ctx => {
  ctx.file = `${DIR}/test-${ctx.dbnum++}.db`
})

test.after.each(ctx => {
  execSync(`rm -f ${ctx.file}`)
})

test('basic', async ctx => {
  const db = new Database(ctx.file)
  await db.insert({ _id: 1, foo: 'bar', ignoreThis: undefined })
  await db.ensureIndex({ name: 'foo', field: 'foo' })

  snapshot('basic.txt', readFileSync(ctx.file, 'utf8'))
})

test('full activity', async ctx => {
  const db = new Database(ctx.file)
  await db.load()
  const date = new Date(2018, 0, 19, 12, 34, 56)
  await db.insert({ _id: 1, foo: 'bar', date })
  let r
  r = await db.find('primary', 1)
  assert.is(r.foo, 'bar')
  r = await db.findOne('primary', 1)
  assert.is(r.foo, 'bar')
  assert.is(+r.date, +date)

  await db.ensureIndex({ name: 'foo', field: 'foo' })
  await db.ensureIndex({ name: 'foo', fields: ['foo'] })
  r = await db.find('foo', 'bar')
  assert.is(r[0]._id, 1)

  r = await db.findOne('foo', 'bar')
  assert.is(r._id, 1)

  await db.insert({ _id: 2, foo: 'bar' })
  r = await db.find('foo', 'bar')
  assert.is(r.length, 2)

  await db.update({ _id: 1, bar: 'quux' })
  r = await db.find('foo', 'bar')
  assert.is(r.length, 1)

  await db.delete({ _id: 1 })

  await db.deleteIndex('foo')
  await db.deleteIndex('primary')

  snapshot('full-activity-1.txt', readFileSync(ctx.file, 'utf8'))

  await db.reload()

  await db.ensureIndex({ name: 'foo', field: 'foo' })
  await db.compact()

  await db.insert({ _id: null, noId: true })
  assert.is((await db.getAll()).length, 2)

  snapshot('full-activity-2.txt', readFileSync(ctx.file, 'utf8'))
})

test('generated id', async ctx => {
  const db = new Database(ctx.file)
  await db.insert({ _id: null, foo: 'bar' })
  snapshot('generated-id-1.txt', readFileSync(ctx.file, 'utf8'))

  await db.insert({ _id: null, foo: 'bar' })
  snapshot('generated-id-2.txt', readFileSync(ctx.file, 'utf8'))

  assert.is((await db.getAll()).length, 2)
})

test('reload', async ctx => {
  const db = new Database(ctx.file)
  await db.insert({ _id: 1, foo: 'bar' })

  assert.is((await db.getAll()).length, 1)

  writeFileSync(ctx.file, '')
  await db.reload()

  assert.is((await db.getAll()).length, 0)
})

test('empty data', async ctx => {
  const db = new Database(ctx.file)
  assert.is(await db.findOne('primary', 1), undefined)
  assert.is(await db.find('primary', 1), undefined)
  await db.ensureIndex({ name: 'foo', field: 'foo' })
  assert.is(await db.findOne('foo', 1), undefined)
  assert.equal(await db.find('foo', 1), [])
})

test('errors', async ctx => {
  assert.throws(() => new Database(), 'No filename given')

  const db = new Database(ctx.file)
  await db
    .delete({ _id: 1 })
    .then(assert.unreachable, err => assert.instance(err, Database.NotExists))
  await db
    .update({ _id: 1 })
    .then(assert.unreachable, err => assert.instance(err, Database.NotExists))

  await db.insert({ _id: 'foo', bar: 'baz' })
  await db
    .insert({ _id: 'foo', bar: 'baz' })
    .then(assert.unreachable, err =>
      assert.instance(err, Database.KeyViolation)
    )

  await db.ensureIndex({ name: 'foo', field: 'foo', unique: true })
  await db.insert({ _id: 1, foo: 'bar' })
  await db
    .insert({ _id: 2, foo: 'bar' })
    .then(assert.unreachable, err =>
      assert.instance(err, Database.KeyViolation)
    )
  assert.equal(await db.find('foo', 'bar'), { _id: 1, foo: 'bar' })

  await db.insert({ _id: 2, foo: 'baz' })
  await db
    .update({ _id: 1, foo: 'baz' })
    .then(assert.unreachable, err =>
      assert.instance(err, Database.KeyViolation)
    )

  await db
    .find('quux', 'l33t')
    .then(assert.unreachable, err => assert.instance(err, Database.NoIndex))
  await db
    .findOne('quux', 'l33t')
    .then(assert.unreachable, err => assert.instance(err, Database.NoIndex))

  await db
    .deleteIndex('quux')
    .then(assert.unreachable, err => assert.instance(err, Database.NoIndex))
})

test('auto compaction', async ctx => {
  const db = new Database(ctx.file)
  db.setAutoCompaction(500)
  await db.insert({ _id: 1 })
  await db.update({ _id: 1, foo: 'bar' })
  await delay(750)
  db.stopAutoCompaction()

  snapshot('auto-compaction.txt', readFileSync(ctx.file, 'utf8'))
})

test('sorted', async ctx => {
  const db = new Database(ctx.file)
  await db.insert({ _id: 'foo', age: 1, name: 'ping' })
  await db.insert({ _id: 'bar', age: 2, name: 'pong' })
  await db.insert({ _id: 'baz', age: 3, name: 'bilbo' })

  snapshot('sorted-none.txt', readFileSync(ctx.file, 'utf8'))

  await db.compact({ sortBy: sortBy('_id') })
  snapshot('sorted-id.txt', readFileSync(ctx.file, 'utf8'))

  await db.compact({ sortBy: sortBy('age') })
  snapshot('sorted-age.txt', readFileSync(ctx.file, 'utf8'))

  await db.compact({ sortBy: sortBy(r => r.name) })
  snapshot('sorted-name.txt', readFileSync(ctx.file, 'utf8'))
})

test('upsert', async ctx => {
  const db = new Database(ctx.file)
  await db.upsert({ _id: 'foo', bar: 'baz' })

  snapshot('upsert-1.txt', readFileSync(ctx.file, 'utf8'))

  await db.upsert({ _id: 'foo', bar: 'quux' })

  snapshot('upsert-2.txt', readFileSync(ctx.file, 'utf8'))
})

test('mulit-row ops', async ctx => {
  const db = new Database(ctx.file)
  let rows = [
    { _id: null, name: 'foo', num: 1 },
    { _id: null, name: 'bar', num: 2 }
  ]
  rows = await db.insert(rows)
  snapshot('multirow-insert.json', rows)

  rows = rows.map(doc => ({ ...doc, num: doc.num * 10 }))
  rows = await db.update(rows)
  snapshot('multirow-update.json', rows)

  snapshot('multirow-db.txt', readFileSync(ctx.file, 'utf8'))

  await db.delete(rows)
  rows = await db.getAll()
  assert.is(0, rows.length)
})

test('frozen objects returned', async ctx => {
  const db = new Database(ctx.file)
  const rec1 = await db.insert({ _id: 1, foo: 'bar' })
  assert.ok(Object.isFrozen(rec1))

  let rec2 = await db.findOne('primary', 1)
  assert.is(rec1, rec2)

  await db.reload()
  rec2 = await db.findOne('primary', 1)
  assert.is.not(rec1, rec2)
  assert.equal(rec1, rec2)
  assert.ok(Object.isFrozen(rec2))
})

test('access db when locked', async ctx => {
  const db1 = new Database(ctx.file)
  await db1.getAll()

  const db2 = new Database(ctx.file)
  await db2
    .getAll()
    .then(assert.unreachable, err =>
      assert.instance(err, Database.DatabaseLocked)
    )
})

test('change primary key', async ctx => {
  const db = new Database(ctx.file)
  await db.insert({ _id: 1, name: 'foo' })
  await db.ensureIndex({ name: 'primary', fields: ['foo']})
  await db.update({ _id: undefined, name: 'foo' })
  snapshot('change-primary-key.txt', readFileSync(ctx.file, 'utf8'))
})

test.run()
