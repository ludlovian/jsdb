'use strict'
import test from 'ava'
import Queue from '../src/queue'

const isResolved = (p, ms = 20) =>
  new Promise(resolve => {
    p.then(() => resolve(true))
    setTimeout(() => resolve(false), ms)
  })

test('queue creation', async t => {
  const q = new Queue()
  t.false(q.started)
})

test('basic add', async t => {
  const q = new Queue()

  const d1 = defer()
  const d2 = defer()
  const p1 = q.add(() => d1)
  const p2 = q.add(() => d2)

  t.false(await isResolved(p1))

  q.start()
  t.false(await isResolved(p1))

  d1.resolve()
  t.true(await isResolved(p1))
  t.false(await isResolved(p2))

  d2.resolve()
  t.true(await isResolved(p2))
})

test('stop', async t => {
  const q = new Queue(true)

  const p1 = q.add(() => 'foo')
  const p2 = q.stop()
  const p3 = q.add(() => 'bar')

  t.true(q.started)

  t.is(await p1, 'foo')
  t.true(await isResolved(p2))
  t.false(await isResolved(p3))
  t.false(q.started)

  q.start()
  t.is(await p3, 'bar')
  t.true(q.started)
})

test('rejecting work item', async t => {
  const e = new Error('oops')
  const q = new Queue(true)
  const p1 = q.add(() => Promise.reject(e))
  await t.throwsAsync(p1, 'oops')

  const p2 = q.add(() => 17)
  t.is(await p2, 17)
})

function defer () {
  let res
  let rej
  const p = new Promise((resolve, reject) => {
    res = resolve
    rej = reject
  })
  p.resolve = res
  p.reject = rej
  return p
}
