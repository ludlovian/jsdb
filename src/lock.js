export default class Lock {
  constructor ({ width = 1 } = {}) {
    this.width = width
    this.count = 0
    this.awaiters = []
  }

  acquire () {
    if (this.count < this.width) {
      this.count++
      return Promise.resolve()
    }
    return new Promise(resolve => this.awaiters.push(resolve))
  }

  release () {
    if (!this.count) return
    if (this.waiting) {
      this.awaiters.shift()()
    } else {
      this.count--
    }
  }

  get waiting () {
    return this.awaiters.length
  }

  async exec (fn) {
    try {
      await this.acquire()
      return await Promise.resolve(fn())
    } finally {
      this.release()
    }
  }
}
