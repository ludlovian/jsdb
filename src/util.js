'use strict'

export function delve (obj, key) {
  let p = 0
  key = key.split('.')
  while (obj && p < key.length) {
    obj = obj[key[p++]]
  }
  return obj === undefined || p < key.length ? undefined : obj
}

export function getId (row, existing) {
  // generate a repeatable for this row, avoiding conflicts with the other rows
  const start = hashString(stringify(row))
  for (let n = 0; n < 1e8; n++) {
    const id = ((start + n) & 0x7fffffff).toString(36)
    if (!existing.has(id)) return id
  }
  // istanbul ignore next
  throw new Error('Could not generate unique id')
}

function hashString (string) {
  return Array.from(string).reduce(
    (h, ch) => ((h << 5) - h + ch.charCodeAt(0)) & 0xffffffff,
    0
  )
}

export function cleanObject (obj) {
  return Object.entries(obj).reduce((o, [k, v]) => {
    if (v !== undefined) o[k] = v
    return o
  }, {})
}

const DATE_SENTINEL = '$jsdb$date$'

export function stringify (obj) {
  return JSON.stringify(obj, function (k, v) {
    return this[k] instanceof Date ? { [DATE_SENTINEL]: this[k].getTime() } : v
  })
}

export function parse (s) {
  return JSON.parse(s, function (k, v) {
    if (k === DATE_SENTINEL) return new Date(v)
    if (typeof v === 'object' && DATE_SENTINEL in v) return v[DATE_SENTINEL]
    return v
  })
}

export function sortOn (selector) {
  if (typeof selector !== 'function') {
    const key = selector
    selector = x => delve(x, key)
  }
  return (a, b) => {
    const x = selector(a)
    const y = selector(b)
    // istanbul ignore next
    return x < y ? -1 : x > y ? 1 : 0
  }
}

export function makeArray (obj) {
  return Array.isArray(obj) ? obj : [obj]
}
