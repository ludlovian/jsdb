export function getId (row, existing) {
  // generate a repeatable for this row, avoiding conflicts with the other rows
  const start = hashString(stringify(row))
  for (let n = 0; n < 1e8; n++) {
    const id = ((start + n) & 0x7fffffff).toString(36)
    if (!existing.has(id)) return id
  }
  /* c8 ignore next */
  throw new Error('Could not generate unique id')
}

function hashString (string) {
  return [...string].reduce(
    (h, ch) => ((h << 5) - h + ch.charCodeAt(0)) & 0xffffffff,
    0
  )
}

export function cleanObject (obj) {
  return Object.fromEntries(
    Object.entries(obj).filter(([, v]) => v !== undefined)
  )
}

export const SEP = String.fromCharCode(31)

const DATE = '$date'

export function stringify (obj) {
  return JSON.stringify(obj, function (k, v) {
    return this[k] instanceof Date ? { [DATE]: this[k].toISOString() } : v
  })
}

export function parse (s) {
  return JSON.parse(s, function (k, v) {
    if (k === DATE) return new Date(v)
    if (v && typeof v === 'object' && DATE in v) return v[DATE]
    return v
  })
}
