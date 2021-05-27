# jsdb
Simple file-based json database

## Overview

A simple database of JSON objects, which are stored in a file.

Basic indexes allow searching. After that you're on your own.

Inspired by the execellent [nedb](https://www.npmjs.com/package/nedb) module, but with:
- much simple API with **far** less functionality
- promises and async as standard
- much smaller footprint
- no predicates, just lookups

Only for my own use. Don't use it. Use nedb instead

When objects are stored in the DB, they are frozen

## API

### Database

`db = new Database(filename)`


### .ensureIndex

`await db.ensureIndex({ fieldName, unique = false, sparse = false })`

Ensures an index is in place.

A `sparse` index will not index null-ish values
A `unique` index will barf on non-unique values

### .deleteIndex

`db.deleteIndex(fieldName)`

removes the index

### .insert

`await db.insert(doc|docs)`

Inserts a new doc or docs. Returns the stored doc/docs with `_id` if not given.

### .update

`await db.update(doc|docs)`

Replaces existing doc (or docs) (based on `_id`) with this new one. Returns the new stored one(s).

### .upsert

`await db.upsert(doc|docs)`

Either insert or update doc (or docs). Returns the new stored one(s).

### .delete

`await db.delete(doc|docs)`

Deletes the doc (or docs) whose `_id` matches this one. Returns the old stored doc.

### .getAll

`allDocs = await db.getAll()`

returns an array of all docs.

### .find

`matching = await db.find(<indexFieldName>, <value>)`

Returns a list of the matching docs which can be empty. Unique indices act like `.findOne` and return a single doc or `undefined`.

### .findOne

`matching = await db.findOne(<indexFieldName>, <value>)`

Returns the first matching doc, or `undefined`

### .compact

`await db.compact(opts)`
comapcts and rewrites the database.

Options:

`sortBy` a sort function with signature `(a,b) => -1|0|+1`


### .setAutoCompaction

`db.setAutoCompaction(30 * 60 * 1000, compactOptions) // 30 minutes`

Sets auto-scheduled compaction

### .stopAutoCompaction

`db.stopAutoCompaction()`

Stops any scheduled compaction

## Errors

Four errors can be thrown. The constructors are found as static properties on `Database`

### NotExists

Thrown when trying to update or delete a record that does not exist.

- `.record` has the missing record

### KeyViolation

Thrown when an insert or update would violate a unique key.

- `.fieldName` has the name of the index
- `.record` has the offending record

### NoIndex

Thrown when a `find` or `findOne` asks to use an index which does not exist

- `.fieldName` has the name of the missing index

### DatabaseLocked

Thrown when a lockfile already exists for this database

- `.filename` is the offending database
