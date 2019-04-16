'use strict'

export function delve (obj, key) {
  let p = 0
  key = key.split('.')
  while (obj && p < key.length) {
    obj = obj[key[p++]]
  }
  return obj === undefined || p < key.length ? undefined : obj
}

function getRandomString (n) {
  return Math.random()
    .toString(36)
    .slice(2, 2 + n)
}

export function getRandomId () {
  return `${getRandomString(6)}-${getRandomString(6)}`
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
