'use strict'

export class DatastoreError extends Error {
  constructor (message) {
    super(message)
    this.name = this.constructor.name
    Error.captureStackTrace(this, this.constructor)
  }
}

export class KeyViolation extends DatastoreError {
  constructor (doc, fieldName) {
    super('Key violation error')
    this.fieldName = fieldName
    this.record = doc
  }
}

export class NotExists extends DatastoreError {
  constructor (doc) {
    super('Record does not exist')
    this.record = doc
  }
}
