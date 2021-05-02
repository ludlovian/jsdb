import { basename } from 'path'
import { symlink } from 'fs/promises'
import { unlinkSync } from 'fs'

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
    throw new Error('Database locked: ' + filename)
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

process
  .on('exit', cleanup)
  .on('SIGINT', cleanup)
  .on('SIGTERM', cleanup)
  .on('uncaughtException', cleanup)
