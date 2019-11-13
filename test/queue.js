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
  t.is(q.running, 0)
  t.is(q.pending, 0)
  t.true(await isResolved(q.wait()))
})

test('basic add', async t => {
  const q = new Queue()

  const d1 = defer()
  const d2 = defer()
  const p1 = q.add(() => d1)
  const p2 = q.add(() => d2)

  t.false(await isResolved(p1))

  d1.resolve()
  t.true(await isResolved(p1))
  t.false(await isResolved(p2))

  d2.resolve()
  t.true(await isResolved(p2))
})

test('wait', async t => {
  const q = new Queue(2)
  const d1 = defer()
  const d2 = defer()
  const p1 = q.add(() => d1)
  const p2 = q.add(() => d2)
  const p3 = q.wait()
  t.false(await isResolved(p3))

  d2.resolve('bar')
  t.false(await isResolved(p3))

  d1.resolve('foo')
  t.is(await p1, 'foo')
  t.is(await p2, 'bar')
  t.true(await isResolved(p3))
})

test('rejecting work item', async t => {
  const e = new Error('oops')
  const q = new Queue(1)
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
