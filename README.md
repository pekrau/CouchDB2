# CouchDB2

Slim Python interface module to CouchDB v2.x.

Relies on `requests`: http://docs.python-requests.org/en/master/

## Installation

Quick-and-dirty method, since this module is not yet on PyPi:
```
$ pip install [-e] git+https://github.com/pekrau/CouchDB2.git#egg=couchdb2
```

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

View all available command options by:

```
$ python couchdb2.py -h
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
- Raises NotFoundError if no such database.

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
- Raises NotFoundError if 'check' is True and no database exists.

### create
```python
server.create(name)
```
Create the named database.
- Raises BadRequestError if the name is invalid.
- Raises AuthorizationError if not server admin privileges.
- Raises CreationError if a database with that name already exists.
- Raises IOError if there is some other error.

### get_config
```python
server.get_config(nodename='_local')
```
Get the named node's configuration.
- Raises AuthorizationError if not server admin privileges.
- Raises IOError if there is some other error.

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
- Raises AuthorizationError if not privileged to read.
- Raises IOError if something else went wrong.

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
- Raises AuthorizationError if not privileged to read.
- Raises NotFoundError if no such document or database.
- Raises IOError if something else went wrong.

### exists
```python
db.exists()
```
Does this database exist?
### check
```python
db.check()
```
- Raises NotFoundError if this database does not exist.
### create
```python
db.create()
```
Create this database.
- Raises BadRequestError if the name is invalid.
- Raises AuthorizationError if not server admin privileges.
- Raises CreationError if a database with that name already exists.
- Raises IOError if there is some other error.

### destroy
```python
db.destroy()
```
Delete this database and all its contents.
- Raises AuthorizationError if not server admin privileges.
- Raises NotFoundError if no such database.
- Raises IOError if there is some other error.

### get_info
```python
db.get_info()
```
Return a dictionary containing information about the database.
### compact
```python
db.compact()
```
Compact the database on disk. May take some time.
### is_compact_running
```python
db.is_compact_running()
```
Is a compact operation running?
### get
```python
db.get(id, rev=None, revs_info=False, default=None)
```
Return the document with the given id.
- Returns the default if not found.
- Raises AuthorizationError if not read privilege.
- Raises IOError if there is some other error.

### save
```python
db.save(doc)
```
Insert or update the document.

If the document does not contain an item '_id', it is added
having a UUID4 value. The '_rev' item is added or updated.

- Raises NotFoundError if the database does not exist.
- Raises AuthorizationError if not privileged to write.
- Raises RevisionError if the '_rev' item does not match.
- Raises IOError if something else went wrong.

### delete
```python
db.delete(doc)
```
Delete the document.
- Raises NotFoundError if no such document or no '_id' item.
- Raises RevisionError if no '_rev' item, or it does not match.
- Raises ValueError if the request body or parameters are invalid.
- Raises IOError if something else went wrong.

### load_design
```python
db.load_design(designname, doc, rebuild=True)
```
Load the design document under the given name.

If the existing design document is identical, no action is taken and
False is returned, else the document is updated and True is returned.

If 'rebuild' is True, force view indexes to be rebuilt after update.

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

- Raises AuthorizationError if not privileged to write.
- Raise NotFoundError if no such database.
- Raises IOError if something else went wrong.

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

- Raises BadRequestError if the index is malformed.
- Raises AuthorizationError if not server admin privileges.
- Raises ServerError if there is an internal server error.

### find
```python
db.find(selector, use_index=None, limit=None, skip=None, sort=None,
        fields=None, bookmark=None, update=None)
```
Select documents according to the Mango index selector.

Returns a dictionary with items 'docs', 'warning', 'execution_stats'
and 'bookmark'.

- Raises BadRequestError if the selector is malformed.
- Raises AuthorizationError if not privileged to read.
- Raises ServerError if there is an internal server error.

### put_attachment
```python
db.put_attachment(doc, content, filename=None, content_type=None)
```
'content' is a string or a file-like object. Return the new revision
of the document.

If no filename, then an attempt is made to get it from content object.

- Raises ValueError if no filename is available.

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
db.dump(filepath, progress_func=None)
```
Dump the entire database to the named tar file.

If defined, the function `progress_func(ndocs, nfiles)` is called 
every 100 documents.

If the filepath ends with `.gz`, then the tar file is gzip compressed.
The `_rev` item of each document is kept.

A tuple `(ndocs, nfiles)` is returned.

### undump
```python
db.undump(filepath, progress_func=None)
```
Load the named tar file, which must have been produced by `dump`.

If defined, the function `progress_func(ndocs, nfiles)` is called 
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
