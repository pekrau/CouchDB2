# CouchDB2

Slim Python interface module for CouchDB v2.x.
Also a [command line tool](#command-line-tool).

## Installation

```
$ pip install couchdb2
```

This module relies on `requests`: http://docs.python-requests.org/en/master/

## Server
```python
server = Server(href='http://localhost:5984/', username=None, password=None, session=True)
```
Connect to the CouchDB server.

If `session` is true, then an authenticated session is set up.
By default, its lifetime is 10 minutes.

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
for db in server: ...
```
Return an iterator over all user-defined databases on the server.

### \_\_getitem\_\_
```python
db = server[name]
```
Get the named database.

### \_\_contains\_\_
```python
if name in server: ...
```
Does the named database exist?

### get
```python
db = server.get(name, check=True)
```
Get the named database. If `check` is true, then raise NotFoundError
if the the database does not exist.

### create
```python
db = server.create(name)
```
Create the named database.

### get_config
```python
data = server.get_config(nodename='_local')
```
Get the named node's configuration.

## Database
```python
db = Database(server, name, check=True)
```
Interface to a named CouchDB database. If `check` is true, then
raise NotFoundError if the the database does not exist.

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
if id in db: ...
```
Does a document with the given id exist in the database?

### \_\_iter\_\_
```python
for doc in db: ...
```
Return an iterator over all documents in the database.

### \_\_getitem\_\_
```python
doc = db[id]
```
Return the document with the given id.

### exists
```python
if db.exists(): ...
```
Does the database exist?

### check
```python
db.check()
```
Raises NotFoundError if the database does not exist.

### create
```python
db.create()
```
Create the database.

### destroy
```python
db.destroy()
```
Delete the database and all its contents.

### get_info
```python
data = db.get_info()
```
Return a dictionary with information about the database.

### get_security
```python
data = db.get_security()
```
Return a dictionary with security information for the database.

### set_security
```python
db.set_security(doc)
```
Set the security information for the database.

### compact
```python
db.compact(finish=False, callback=None)
```
Compact the CouchDB database by rewriting the disk database file
and removing old revisions of documents.

If `finish` is True, then return only when compaction is done.
In addition, if defined, the function `callback(seconds)` is called
every second until compaction is done.

### compact
```python
db.compact_design(designname)
```
Compact the view indexes associated with the named design document.

### view_cleanup
```python
db.view_cleanup()
```
Remove unnecessary view index files due to changed views in
design documents of the database.

### get
```python
doc = db.get(id, rev=None, revs_info=False, default=None)
```
Return the document with the given id, or the `default` value if not found.

### put
```python
db.put(doc)
```
Insert or update the document.

If the document is already in the database, the `_rev` item must
be present in the document, and it will be updated.

If the document does not contain an item `_id`, it is added
having a UUID4 value. The `_rev` item is also added.

### delete
```python
db.delete(doc)
```
Delete the document.

### get_designs
```python
data = db.get_designs()
```
Return the design documents for the database.

### get_design
```python
data = db.get_design(designname)
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
  {"views":
    {"name":
      {"map": "function (doc) {emit(doc.name, null);}"},
     "name_sum":
      {"map": "function (doc) {emit(doc.name, 1);}",
       "reduce": "_sum"},
     "name_count":
      {"map": "function (doc) {emit(doc.name, null);}",
       "reduce": "_count"}
  }}
```

More info: http://docs.couchdb.org/en/latest/api/ddoc/common.html

### view
```python
result = db.view(designname, viewname, key=None, keys=None,
                 startkey=None, endkey=None, skip=None, limit=None,
                 sorted=True, descending=False,
                 group=False, group_level=None, reduce=None,
                 include_docs=False)
```
Return a [ViewResult](#viewresult) object, containing
[Row](#row) objects in the list attribute `rows`.

### put_index
```python
db.put_index(fields, ddoc=None, name=None, selector=None)
```
Store a Mango index specification.

- `fields` is a list of fields to index.
- `ddoc` is the design document name. Generated if none given.
- `name` is the name of the index. Generated if none given.
- `selector` is a partial filter selector, which may be omitted.

Returns a dictionary with items `id` (design document name; sic!),
`name` (index name) and `result` (`created` or `exists`).

### find
```python
data = db.find(selector, use_index=None, limit=None, skip=None, sort=None,
               fields=None, bookmark=None, update=None)
```
Select documents according to the Mango index selector.

Returns a dictionary with items `docs`, `warning`, `execution_stats`
and `bookmark`.

### get_attachment
```python
fileobj = db.get_attachment(doc, filename)
```
Return a file-like object containing the content of the attachment.

### put_attachment
```python
rev = db.put_attachment(doc, content, filename=None, content_type=None)
```
`content` is a string or a file-like object. Return the new revision
of the document.

If no filename, then an attempt is made to get it from content object.

### delete_attachment
```python
rev 0 db.delete_attachment(doc, filename)
```
Delete the attachment. Return the new revision of the document.

### dump
```python
(ndocs, nfiles) = db.dump(filepath, callback=None)
```
Dump the entire database to a tar file.

If defined, the function `callback(ndocs, nfiles)` is called 
every 100 documents.

If the filepath ends with `.gz`, then the tar file is gzip compressed.
The `_rev` item of each document is kept.

A tuple `(ndocs, nfiles)` is returned.

### undump
```python
(ndocs, nfiles) = db.undump(filepath, callback=None)
```
Load the tar file, which must have been produced by `dump`.

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
Wrong or missing `_rev` item in the document to save.
## AuthorizationError
```python
AuthorizationError()
```
Current user not authorized to perform the operation.
## ContentTypeError
```python
ContentTypeError()
```
Bad `Content-Type` value in the request.
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
- `offset`: the offset used for the set of rows.
- `total_rows`: the total number of rows selected.

### rows
Contains the rows found: list of `Row` objects.

Alias for field number 0
### offset
Alias for field number 1
### total_rows
Alias for field number 2

## get_settings
```python
get_settings(filepath, settings=None)
```
Get the settings lookup from a JSON format file.
If `settings` is given, then output an updated copy of it,
else update the default settings.

## Command line tool

The module is also a command line tool for interacting with the CouchDB server.

```
$ python couchdb2.py -h
```

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
4) From the file given by command line option `--settings FILEPATH`.

Available command options:

```
usage: couchdb2.py [-h] [--settings SETTINGS] [-S SERVER] [-d DATABASE]
                   [-u USERNAME] [-p PASSWORD] [-q] [-o FILEPATH]
                   [--indent INT] [-y] [-v | -s] [-V] [--list]
                   [--create | --destroy] [--compact] [--compact_design DDOC]
                   [--view_cleanup] [--info]
                   [--security | --set_security FILEPATH]
                   [--list_designs | --design DDOC | --put_design DDOC FILEPATH | --delete_design DDOC]
                   [--dump FILEPATH | --undump FILEPATH]
                   [-G DOCID | -P FILEPATH | --delete DOCID]
                   [--attach DOCID FILEPATH | --detach DOCID FILENAME | --get_attach DOCID FILENAME]
                   [--view SPEC] [--key KEY | --startkey KEY] [--endkey KEY]
                   [--startkey_docid DOCID] [--endkey_docid DOCID] [--group]
                   [--group_level INT] [--noreduce] [--limit INT] [--skip INT]
                   [--descending] [--include_docs]

CouchDB2 command line tool

optional arguments:
  -h, --help            show this help message and exit
  --settings SETTINGS   settings file in JSON format
  -S SERVER, --server SERVER
                        CouchDB server URL, including port number
  -d DATABASE, --database DATABASE
                        database to operate on
  -u USERNAME, --username USERNAME
                        CouchDB user account name
  -p PASSWORD, --password PASSWORD
                        CouchDB user account password
  -q, --password_question
                        ask for the password by interactive input
  -o FILEPATH, --output FILEPATH
                        write output to the given file (JSON format)
  --indent INT          indentation level for JSON format output file
  -y, --yes             do not ask for confirmation (delete, destroy)
  -v, --verbose         print more information
  -s, --silent          print no information

server operations:
  -V, --version         output CouchDB server version
  --list                output a list of the databases on the server

database operations:
  --create              create the database
  --destroy             delete the database and all its contents
  --compact             compact the database; may take some time
  --compact_design DDOC
                        compact the view indexes for the named design doc
  --view_cleanup        remove view index files no longer required
  --info                output information about the database
  --security            output security information for the database
  --set_security FILEPATH
                        set security information for the database from the
                        JSON file
  --list_designs        list design documents for the database
  --design DDOC         output the named design document
  --put_design DDOC FILEPATH
                        store the named design document from the file
  --delete_design DDOC  delete the named design document
  --dump FILEPATH       create a dump file of the database
  --undump FILEPATH     load a dump file into the database

document operations:
  -G DOCID, --get DOCID
                        output the document with the given identifier
  -P FILEPATH, --put FILEPATH
                        store the document; arg is literal doc or filepath
  --delete DOCID        delete the document with the given identifier

attachments to document:
  --attach DOCID FILEPATH
                        attach the specified file to the given document
  --detach DOCID FILENAME
                        remove the attached file from the given document
  --get_attach DOCID FILENAME
                        get the attached file from the given document; write
                        to same filepath or that given by '-o'

query a design view, returning rows:
  --view SPEC           design view '{design}/{view}' to query
  --key KEY             key value selecting view rows
  --startkey KEY        start key value selecting range of view rows
  --endkey KEY          end key value selecting range of view rows
  --startkey_docid DOCID
                        return rows starting with the specified document
  --endkey_docid DOCID  stop returning rows when specified document reached
  --group               group the results using the 'reduce' function
  --group_level INT     specify the group level to use
  --noreduce            do not use the 'reduce' function of the view
  --limit INT           limit the number of returned rows
  --skip INT            skip this number of rows before returning result
  --descending          sort rows in descending order (swap start/end keys!)
  --include_docs        include documents in result
```
