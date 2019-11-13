'use strict'

const resolved = Promise.resolve()

export default class Queue {
  constructor (width = 1) {
    let running = 0
    const waiting = []
    const listeners = []

    Object.defineProperties(this, {
      running: {
        get: () => running
      },
      pending: {
        get: () => waiting.length
      }
    })

    this.add = fn =>
      new Promise((resolve, reject) => {
        const job = { fn, resolve, reject }
        if (running < width) startJob(job)
        else waiting.push(job)
      })

    this.wait = () =>
      !running ? resolved : new Promise(resolve => listeners.push(resolve))

    function startJob ({ fn, resolve, reject }) {
      running++
      resolved
        .then(() => fn())
        .then(resolve, reject)
        .then(endJob)
    }

    function endJob () {
      if (--running < width && waiting.length) {
        startJob(waiting.shift())
      }
      if (running === 0) {
        listeners.splice(0).map(resolve => resolve())
      }
    }
  }
}
