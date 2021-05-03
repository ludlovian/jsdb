import { basename } from 'path'
import { symlink } from 'fs/promises'
import { unlinkSync } from 'fs'

import { DatabaseLocked } from './errors.mjs'

const lockfiles = new Set()

export async function lockFile (filename) {
  const lockfile = filename + '.lock~'
  const target = basename(filename)
  try {
    await symlink(target, lockfile)
    lockfiles.add(lockfile)
  } catch (err) {
    /* c8 ignore next */
    if (err.code !== 'EEXIST') throw err
    throw new DatabaseLocked(filename)
  }
}

function cleanup () {
  lockfiles.forEach(file => {
    try {
      unlinkSync(file)
    } catch {
      // pass
    }
  })
}

function cleanAndGo () {
  cleanup
  setImmediate(() => process.exit(2))
}

process
  .on('exit', cleanup)
  .on('SIGINT', cleanAndGo)
