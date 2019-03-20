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
    .toString(16)
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
