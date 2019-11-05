'use strict'

export default class Queue {
  constructor (start) {
    this._resetReadyFlag()

    // empty queue just wait until we are ready
    this._head = this._ready
    if (start) this.start()
  }

  add (fn) {
    const waitForReady = () => this._ready
    // the consumers promise which is returned
    const prom = this._head.then(fn)
    // the queue which swallows any error and waits to continue
    this._head = prom.then(waitForReady, waitForReady)
    return prom
  }

  stop () {
    return this.add(() => this._resetReadyFlag())
  }

  _resetReadyFlag () {
    this.started = false
    this._ready = new Promise(resolve => {
      this.start = () => {
        this.started = true
        resolve()
      }
    })
  }
}
