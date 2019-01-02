# CouchDB2

Slim Python interface module for CouchDB v2.x.
Also contains a [command line tool](#command-line-tool)

## Installation

This module relies on `requests`: http://docs.python-requests.org/en/master/

Quick-and-dirty installation method, since this module is not yet on PyPi:
```
$ pip install [-e] git+https://github.com/pekrau/CouchDB2.git#egg=couchdb2
```

## Server
```python
server = Server(href='http://localhost:5984/', username=None, password=None)
```
A connection to the CouchDB server.
### \_\_str\_\_
```python
str(server)
```
Return a simple string representation of the server interface.

### \_\_len\_\_
```python
len(server)
```
Return the number of user-defined databases.

### \_\_iter\_\_
```python
iter(server)
```
Return an iterator over all user-defined databases on the server.

### \_\_getitem\_\_
```python
server[name]
```
Get the named database.

### \_\_contains\_\_
```python
name in server
```
Does the named database exist?

### get
```python
server.get(name, check=True)
```
Get the named database.

### create
```python
server.create(name)
```
Create the named database.

### get_config
```python
server.get_config(nodename='_local')
```
Get the named node's configuration.

## Database
```python
db = Database(server, name, check=True)
```
Interface to a named CouchDB database.

### \_\_str\_\_
```python
str(db)
```
Return the name of the CouchDB database.

### \_\_len\_\_
```python
len(db)
```
Return the number of documents in the database.

### \_\_contains\_\_
```python
id in db
```
Does a document with the given id exist in the database?

### \_\_iter\_\_
```python
iter(db)
```
Iterate over all documents in the database.

### \_\_getitem\_\_
```python
db[id]
```
Return the document with the given id.

### exists
```python
db.exists()
```
Does this database exist?

### check
```python
db.check()
```
Raises NotFoundError if this database does not exist.

### create
```python
db.create()
```
Create this database.

### destroy
```python
db.destroy()
```
Delete this database and all its contents.

### get_info
```python
db.get_info()
```
Return a dictionary containing information about the database.

### compact
```python
db.compact(finish=False, callback=None)
```
Compact the CouchDB database.

If `finish` is True, then return only when compaction is done.
If defined, the function `callback(seconds)` is called it every
second until compaction is done.

### get
```python
db.get(id, rev=None, revs_info=False, default=None)
```
Return the document with the given id, or the `default` value if not found.

### put
```python
db.put(doc)
```
Insert or update the document. If the document is already in 
the database, the `_rev` item must be present in the document.

If the document does not contain an item `_id`, it is added
having a UUID4 value. The `_rev` item is added or updated.

### delete
```python
db.delete(doc)
```
Delete the document.

### get_designs
```python
db.get_designs()
```
Get the design documents for the database.

### get_design
```python
db.get_design(designname)
```
Get the named design document.

### put_design
```python
db.put_design(designname, doc, rebuild=True)
```
Insert or update the design document under the given name.

If the existing design document is identical, no action is taken and
False is returned, else the document is updated and True is returned.

If `rebuild` is True, force view indexes to be rebuilt after update.

Example of doc:
```
  {'views':
    {'name':
      {'map': "function (doc) {emit(doc.name, null);}"},
     'name_sum':
      {'map': "function (doc) {emit(doc.name, 1);}",
       'reduce': '_sum'},
     'name_count':
      {'map': "function (doc) {emit(doc.name, null);}",
       'reduce': '_count'}
  }}
```

More info: http://docs.couchdb.org/en/latest/api/ddoc/common.html

### view
```python
db.view(designname, viewname, key=None, keys=None, startkey=None, endkey=None,
        skip=None, limit=None, sorted=True, descending=False,
        group=False, group_level=None, reduce=None,
        include_docs=False)
```
Return a [ViewResult](#viewresult) object, containing
[Row](#row) objects in the list attribute `rows`.

### load_index
```python
db.load_index(fields, ddoc=None, name=None, selector=None)
```
Load a Mango index specification.

- 'fields' is a list of fields to index.
- 'ddoc' is the design document name. Generated if none given.
- 'name' is the name of the index. Generated if none given.
- 'selector' is a partial filter selector, which may be omitted.

Returns dictionary with items 'id' (design document name; sic!),
'name' (index name) and 'result' ('created' or 'exists').

### find
```python
db.find(selector, use_index=None, limit=None, skip=None, sort=None,
        fields=None, bookmark=None, update=None)
```
Select documents according to the Mango index selector.

Returns a dictionary with items 'docs', 'warning', 'execution_stats'
and 'bookmark'.

### put_attachment
```python
db.put_attachment(doc, content, filename=None, content_type=None)
```
'content' is a string or a file-like object. Return the new revision
of the document.

If no filename, then an attempt is made to get it from content object.

### get_attachment
```python
db.get_attachment(doc, filename)
```
Return a file-like object containing the content of the attachment.

### delete_attachment
```python
db.delete_attachment(doc, filename)
```
Delete the attachment. Return the new revision of the document.

### dump
```python
db.dump(filepath, callback=None)
```
Dump the entire database to the named tar file.

If defined, the function `callback(ndocs, nfiles)` is called 
every 100 documents.

If the filepath ends with `.gz`, then the tar file is gzip compressed.
The `_rev` item of each document is kept.

A tuple `(ndocs, nfiles)` is returned.

### undump
```python
db.undump(filepath, callback=None)
```
Load the named tar file, which must have been produced by `dump`.

If defined, the function `callback(ndocs, nfiles)` is called 
every 100 documents.

NOTE: The documents are just added to the database, ignoring any
`_rev` items.

A tuple `(ndocs, nfiles)` is returned.

## CouchDB2Exception
```python
CouchDB2Exception()
```
Base CouchDB2 exception.
## NotFoundError
```python
NotFoundError()
```
No such entity exists.
## BadRequestError
```python
BadRequestError()
```
Invalid request; bad name, body or headers.
## CreationError
```python
CreationError()
```
Could not create the entity; it exists already.
## RevisionError
```python
RevisionError()
```
Wrong or missing '_rev' item in the document to save.
## AuthorizationError
```python
AuthorizationError()
```
Current user not authorized to perform the operation.
## ContentTypeError
```python
ContentTypeError()
```
Bad 'Content-Type' value in the request.
## ServerError
```python
ServerError()
```
Internal server error.
## Row

Named tuple object returned in ViewResult list attribute `rows`.

```python
Row(id, key, value, doc)
```

- `id`: the identifier of the document, if any.
- `key`: the key for the index row.
- `value`: the value for the index row.
- `doc`: the document, if any.

### id
Alias for field number 0
### key
Alias for field number 1
### value
Alias for field number 2
### doc
Alias for field number 3

## ViewResult

Named tuple object returned as result from `db.view()`.

```python
ViewResult(rows, offset, total_rows)
```
- `rows`: the list of `Row` objects.
- `offset`: the offset used for this set of rows.
- `total_rows`: the total number of rows selected.

### rows
Contains the rows found: list of `Row` objects.

Alias for field number 0
### offset
Alias for field number 1
### total_rows
Alias for field number 2

## Command line tool

Interact with the CouchDB server via the command line.

Settings for the command line tool are updated in order from the following
sources (if existing):

1) Defaults are
   ```
   {
     "SERVER": "http://localhost:5984",
     "DATABASE": null,
     "USERNAME": null,
     "PASSWORD": null
   }
   ```
2) From JSON file `~/.couchdb2`
3) From JSON file `settings.json` (in the current working directory).
4) From JSON file `--settings file`, if given.

Available command options:

```
```
