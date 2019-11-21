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

## API

### Datastore

`db = new Datastore(filename | options)`

Where options are

- `filename` - the filename
- `autoload` - autoload it
- `autocompact` - auto compaction interval

### .load

`await db.load()`

Reads the file into memory and prepares indexes. All other IO is queued until this completes.

### .ensureIndex

`await db.ensureIndex({ fieldName, unique = false, sparse = false })`

Ensures an index is in place.

A `sparse` index will not index null-ish values
A `unique` index will barf on non-unique values

### .removeIndex

`db.removeIndex(fieldName)`

removes the index

### .insert

`await db.insert(doc)`

Inserts a new doc. Returns the actual stored doc with `_id` if not given.

### .update

`await db.update(updatedDoc)`

Replaces existing doc (based on `_id`) with this new one. Returns the new stored one.

### .delete

`await db.delete(doc)`

Deletes the doc whose `_id` matches this one. Returns the old stored doc.

### .getAll

`allDocs = await db.getAll()`

returns an array of all docs.

### .compact

`await db.compact(opts)`
comapcts and rewrites the database.

Options:

`sorted` if set, sorts the records in _id order (default: false)

### .setAutoCompaction

`db.setAutoCompaction(30 * 60 * 1000) // 30 minutes`

Sets auto-scheduled compaction

### .stopAutoCompaction

`db.stopAutoCompaction()`

Stops any scheduled compaction

### Index.find

`matching = await db.indexes.<fieldName>.find(<value>)`

Returns a list of the matching docs which can be empty. Unique indices act like `findOne` and return a single doc or `undefined`.

### Index.findOne

`matching = await db.indexes.<fieldName>.findOne(<value>)`

Returns the first matching doc, or `undefined`

### Index.getAll

`allIndexedDocs = await db.indexes.<fieldName>.getAll()`

returns an array of `[key, [matchingDocs]]` for the index.

Unique indices return an array of `[key, doc]`
