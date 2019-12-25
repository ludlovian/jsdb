import test from 'ava'
import Lock from '../src/lock'

const isResolved = (p, ms = 20) =>
  new Promise(resolve => {
    p.then(() => resolve(true))
    setTimeout(() => resolve(false), ms)
  })

test('acquire and release', async t => {
  const l = new Lock()

  t.is(l.count, 0)
  await l.acquire()

  t.is(l.count, 1)
  l.release()

  t.is(l.count, 0)
})

test('waiting for lock', async t => {
  const l = new Lock()
  await l.acquire()

  const p1 = l.acquire()
  t.false(await isResolved(p1))

  l.release()
  t.is(l.count, 1)
  t.true(await isResolved(p1))

  l.release()
  t.is(l.count, 0)
})

test('multiple width lock', async t => {
  const l = new Lock({ width: 2 })

  await l.acquire()
  t.is(l.count, 1)

  await l.acquire()
  t.is(l.count, 2)

  const p3 = l.acquire()
  t.is(l.waiting, 1)
  t.false(await isResolved(p3))

  l.release()
  t.is(l.waiting, 0)
  t.is(l.count, 2)

  l.release()
  t.is(l.count, 1)

  await l.acquire()
  l.release()
  l.release()
  t.is(l.count, 0)
})

test('over-release lock', async t => {
  const l = new Lock()
  await l.acquire()
  l.release()
  t.is(l.count, 0)
  l.release()
  t.is(l.count, 0)
})

test('exec', async t => {
  const l = new Lock()
  await l.acquire()

  const p = l.exec(() => 17)
  t.false(await isResolved(p))

  l.release()
  t.true(await isResolved(p))
  t.is(await p, 17)

  const e = new Error('oops')

  const err = await t.throwsAsync(() =>
    l.exec(() => {
      throw e
    })
  )

  t.is(err, e)
  t.is(l.count, 0)
})
