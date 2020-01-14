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

NOTE: `findAll` has been removed

## API

### Database

`db = new Database(filename | options)`

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

### .deleteIndex

`db.deleteIndex(fieldName)`

removes the index

### .insert

`await db.insert(doc|docs)`

Inserts a new doc or docs. Returns the actual stored doc/docs with `_id` if not given.

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

`sorted` sorts the records (default: false)

`sorted` can be a field name, a selector function, or if truthy then the same as if `_id` was given

### .setAutoCompaction

`db.setAutoCompaction(30 * 60 * 1000) // 30 minutes`

Sets auto-scheduled compaction

### .stopAutoCompaction

`db.stopAutoCompaction()`

Stops any scheduled compaction

