'use strict'

export class DatastoreError extends Error {
  constructor (name, message) {
    super(message)
    this.name = name
    Error.captureStackTrace(this, this.constructor)
  }
}

export class DatabaseLocked extends DatastoreError {
  constructor (filename) {
    super('DatabaseLocked', 'Database is locked')
    this.filename = filename
  }
}

export class KeyViolation extends DatastoreError {
  constructor (doc, fieldName) {
    super('KeyViolation', 'Key violation error')
    this.fieldName = fieldName
    this.record = doc
  }
}

export class NotExists extends DatastoreError {
  constructor (doc) {
    super('NotExists', 'Record does not exist')
    this.record = doc
  }
}

export class NoIndex extends DatastoreError {
  constructor (fieldName) {
    super('NoIndex', 'No such index')
    this.fieldName = fieldName
  }
}
