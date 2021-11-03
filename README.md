# CouchDB2

CouchDB v2.x Python 3 interface in a single module.
Also a command line tool; [see below](#command-line-tool).

Most, but not all, features of this module work with CouchDB version < 2.0.

## Installation

The CouchDB2 module is available at [PyPi](https://pypi.org/project/CouchDB2/).
It can be installed using [pip](https://pip.pypa.io/en/stable/):

```
$ pip install couchdb2
```

The module relies on `requests`: http://docs.python-requests.org/en/master/
and `tqdm`: https://tqdm.github.io/

## Example code

```python
import couchdb2

server = couchdb2.Server()   # Arguments required according to local setup
db = server.create("test")

doc1 = {"_id": "myid", "name": "mydoc", "level": 4}
db.put(doc1)
doc = db["myid"]
assert doc == doc1

doc2 = {"name": "another", "level": 0}
db.put(doc2)
print(doc2)
# {"_id": "66b5...", "_rev": "1-f3ac...", "name": "another", "level": 0}

db.put_design("mydesign", 
              {"views":
               {"name": {"map": "function (doc) {emit(doc.name, null);}"}
               }
              })
result = db.view("mydesign", "name", key="another", include_docs=True)
assert len(result) == 1
print(result[0].doc)         # Same printout as above, using OrderedDict

db.destroy()
```

## News

- 1.12.0
  - Added progressbar to `dump` and `undump`, using `tqdm`.
  - Corrected bug fetching attachments with names having "weird" characters.
  - Corrected bug creating partitioned database; thanks to https://github.com/N-Vlahovic
- 1.11.0
  - Added `db.changes()`. **But not adequately tested!**
  - Corrected returned value from `set_replicate`: JSON data rather than
    response from `requests`.
  - Refactored code: Got rid of `object_pairs_hook=dict` in `json()`.
    `response.json()` on a separate line everywhere.
- 1.10.0
  - Changed to unittest for the test script. Got rid of package `pytest`.
    Added some tests.
  - Improved the documentation in source and README.
  - Fixed setup.py.
  - Added `__del__` to Server class.
  - Simplified `get_scheduler_docs`.
  - Hid module-level functions which are just help functions for the
    command-line tool.
  - Removed `CouchDB version >= 2.0` from some methods incorrectly
    marked as such.
  - Changed signatures of class Database methods `__init__`, `create`,
    `find` and `explain`.
- 1.9.5
  - Added `__str__` to Server class.
- 1.9.4
  - Added `update` parameter on views; thanks to https://github.com/rbarreiro
  - Added `n` and `q` parameters when creating a database; thanks to
    https://github.com/elChapoSing
- 1.9.3
  - Handle `get_attachment` rev/If-Match issue; thanks to https://github.com/seb4itik
- 1.9.2
  - Added retrieve of conflicts; thanks to https://github.com/seb4itik
- 1.9.1
  - Added `ca_file` parameter for HTTPS; thanks to https://github.com/seb4itik
- 1.9.0
  - Changed `put_attachment` and `delete_attachment` to update the input
    `doc` by the new `_rev` value.
- 1.8.5
  - Added `ids()`: Return an iterator over all document identifiers.
- 1.8.4
  - Added `get_bulk(ids)`: Get several documents in one operation.

## `class Server`
```python
server = Server(href="http://localhost:5984/",
                username=None, password=None,
                use_session=True, ca_file=None)
```
An instance of the class is a connection to the CouchDB server.

- `href` is the URL to the CouchDB server itself.
- `username` and `password` specify the CouchDB user account to use.
- If `use_session` is `True`, then an authenticated session is used
  transparently. Otherwise, the values of `username` and `password` are
  sent with each request.
- `ca_file` is a path to a file or a directory containing CAs if
  you need to access databases in HTTPS.

### `server.version`

Property attribute providing the version of the CouchDB server software.

### `server.user_context`

Property attribute providing the user context of the connection.

### `__str__`
```python
str(server)
```
Returns a simple string representation of the server interface.

### `__len__`
```python
len(server)
```
Returns the number of user-defined databases.

### `__iter__`
```python
for db in server: ...
```
Returns an iterator over all user-defined databases on the server.

### `__getitem__`
```python
db = server[name]
```
Gets the named database.

### `__contains__`
```python
if name in server: ...
```
Does the named database exist?

### `__call__`
```python
data = server()
```
Returns meta information about the server.

### `__del__`

Clean-up: Closes the `requests` session.

Not for explicit use; it is automatically called when the Python
garbage collection mechanism deletes the instance.

### `server.up()`

Is the server up and running, ready to respond to requests?
Returns a boolean.

*CouchDB version >= 2.0*

### `server.get(name, check=True)`

Gets the named database. Returns an instance of class `Database`.

Raises `NotFoundError` if `check` is `True` and the database does not exist.

### `server.create(name, n=3, q=8, partitioned=False)`

Creates the named database. Raises `CreationError` if it already exists.

- `name`: The name of the database.
- `n`: The number of replicas.
- `q`: The number of shards.
- `partitioned`: Whether to create a partitioned database.

### `server.get_config(nodename="_local")`

Gets the named node's configuration.

### `server.get_active_tasks()`

Returns a list of running tasks.

### `server.get_cluster_setup(ensure_dbs_exists=None)`

Returns the status of the node or cluster.

`ensure_dbs_exists` is a list system databases to ensure exist on the
node/cluster. Defaults to `["_users","_replicator"]`.

*CouchDB version >= 2.0*

### `server.set_cluster_setup(doc)`

Configures a node as a single node, as part of a cluster, or finalise a
cluster.

See the CouchDB documentation for the contents of `doc`.

*CouchDB version >= 2.0*

### `server.get_membership()`

Returns data about the nodes that are part of the cluster.

*CouchDB version >= 2.0*

### `server.set_replicate(doc)`

Request, configure, or stop, a replication operation.

See the CouchDB documentation for the contents of `doc`.

### `server.get_scheduler_jobs(limit=None, skip=None)`

Gets a list of replication jobs.

- `limit`: How many results to return.
- `skip`: How many result to skip starting at the beginning,
  ordered by replication ID.

### `server.get_scheduler_docs(limit=None, skip=None)`

Gets information about replication document states.

- `limit`: How many results to return.
- `skip`: How many result to skip starting at the beginning,
  ordered by document ID.

### `server.get_node_stats(nodename="_local")`

Returns statistics for the running server.

### `server.get_node_system(nodename="_local")`

Returns various system-level statistics for the running server.

## `class Database`
```python
db = Database(server, name, check=True)
```

An instance of the class is an interface to a CouchDB database.

- `server`: An instance of Server.
- `name`: The name of the database.
- If `check` is `True`, then raise `NotFoundError` if the the database
  does not exist.

### `__str__`
```python
str(db)
```
Returns the name of the CouchDB database.

### `__len__`
```python
len(db)
```
Returns the number of documents in the database.

### `__contains__`
```python
if identifier in db: ...
```
Does a document with the given identifier exist in the database?

### `__iter__`
```python
for doc in db: ...
```
Returns an iterator over all documents in the database.

### `__getitem__`
```python
doc = db[id]
```
Returns the document with the given id.

### `db.exists()`

Does the database exist? Returns a boolean.

### `db.check()`

Raises `NotFoundError` if the database does not exist.

### `db.create(n=3, q=8, partitioned=False)`

Creates the database. Raises `CreationError` if it already exists.

- `n`: The number of replicas.
- `q`: The number of shards.
- `partitioned`: Whether to create a partitioned database.

### `db.destroy()`

Deletes the database and all its contents.

### `db.get_info()`

Returns a dictionary with information about the database.

### `db.get_security()`

Returns a dictionary with security information for the database.

### `db.set_security(doc)`

Sets the security information for the database.

See the CouchDB documentation for the contents of `doc`.

### `db.compact(finish=False, callback=None)`

Compacts the CouchDB database by rewriting the disk database file
and removing old revisions of documents.

- If `finish` is `True`, then return only when compaction is done.
- In addition, if defined, the function `callback(seconds)` is called
  every second until compaction is done.

### `db.compact_design(designname)`

Compacts the view indexes associated with the named design document.

### `db.view_cleanup()`

Removes unnecessary view index files due to changed views in
design documents of the database.

### `db.get(id, default=None, rev=None, revs_info=False, conflicts=False)`

Returns the document with the given identifier, or the `default` value
if not found.

- `rev`: Retrieves document of specified revision, if specified.
- `revs_info`: Whether to include detailed information for all known
  document revisions.
- `conflicts`: Whether to include information about conflicts in
  the document in the `_conflicts` attribute.

### `db.get_bulk(ids)`

Gets several documents in one operation, given a list of document identifiers,
each of which is a string (the document `_id`), or a tuple of the
document `(_id, _rev)`.

Returns a list of documents. If no document is found for a specified
`_id` or `(_id, _rev`), `None` is returned in that slot of the list.

### `db.ids()`
```python
for identifier in db.ids(): ...
```

Returns an iterator over all document identifiers.

### `db.put(doc)`

Inserts or updates the document.

If the document is already in the database, the `_rev` item must
be present in the document; its value will be updated.

If the document does not contain an item `_id`, it is added
having a UUID4 hex value. The `_rev` item will also be added.

### `db.update(docs)`

Performs a bulk update or insertion of the given documents using a
single HTTP request.

Returns an iterable (list) over the resulting documents.

`docs` is a sequence of dictionaries or `Document` objects, or
objects providing an `items()` method that can be used to convert
them to a dictionary.

The return value of this method is a list containing a tuple for every
element in the `docs` sequence. Each tuple is of the form
`(success, docid, rev_or_exc)`, where `success` is a boolean
indicating whether the update succeeded, `docid` is the ID of the
document, and `rev_or_exc` is either the new document revision, or
an exception instance (e.g. `ResourceConflict`) if the update failed.

If an object in the documents list is not a dictionary, this method
looks for an `items()` method that can be used to convert the object
to a dictionary.
                  
### `db.delete(doc)`

Deletes the document, which must contain the `_id` and `_rev` items.

### `db.purge(docs)`

Performs purging (complete removal) of the given list of documents.

Uses a single HTTP request to purge all given documents. Purged
documents do not leave any meta-data in the storage and are not
replicated.

### `db.get_designs()`

Returns the design documents for the database.

**NOTE:** *CouchDB version >= 2.2*

### `db.get_design(designname)`

Gets the named design document.

### `db.put_design(designname, doc, rebuild=True)`

Inserts or updates the design document under the given name.

If the existing design document is identical, no action is taken and
`False` is returned, else the document is updated and `True` is returned.

If `rebuild` is `True`, force view indexes to be rebuilt after update
by accessing the view. This may take some time.

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

### `db.view(designname, viewname, **kwargs)`

Query a view index to obtain data and/or documents.

Keyword arguments (default value is `None` unless specified):

- `key`: Return only rows that match the specified key.
- `keys`: Return only rows where they key matches one of those specified as a list.
- `startkey`: Return rows starting with the specified key.
- `endkey`: Stop returning rows when the specified key is reached.
- `skip`: Skip the number of rows before starting to return rows.
- `limit`: Limit the number of rows returned.
- `sorted=True`: Sort the rows by the key of each returned row. The items
  `total_rows` and `offset` are not available if set to `False`.
- `descending=False`: Return the rows in descending order.
- `group=False`: Group the results using the reduce function of the design
  document to a group or a single row. If set to `True` implies `reduce`
  to `True` and defaults the `group_level` to the maximum.
- `group_level`: Specify the group level to use. Implies `group` is `True`.
- `reduce`: If set to `False` then do not use the reduce function if there is
  one defined in the design document. Default is to use the reduce function
  if defined.
- `include_docs=False`: If `True`, include the document for each row. This
  will force `reduce` to `False`.
- `update="true"`: Whether ir not the view should be updated prior to
  returning the result. Supported value are `"true"`, `"false"` and `"lazy"`.

Returns an instance of [class ViewResult](#class-viewresultrows-offset-total_rows),
containing the following attributes:

- `rows`: The list of `Row` objects.
- `offset`: The offset used for the set of rows.
- `total_rows`: The total number of rows selected.

A Row object contains the following attributes:

- `id`: The identifier of the document, if any.
- `key`: The key for the index row.
- `value`: The value for the index row.
- `doc`: The document, if any.

### `db.get_indexes()`

Returns a list of all Mango indexes in the database.

*CouchDB version >= 2.0*

### `db.put_index(fields, ddoc=None, name=None, selector=None)`

Stores a Mango index specification.

- `fields`: A list of fields to index.
- `ddoc`: The design document name. Generated if none given.
- `name`: The name of the index. Generated if none given.
- `selector`: A partial filter selector, which may be omitted.

Returns a dictionary with items `id` (design document name; sic!),
`name` (index name) and `result` (`created` or `exists`).

*CouchDB version >= 2.0*

### `db.delete_index(designname, name)`

Deletes the named index in the design document of the given name.

*CouchDB version >= 2.0*

### `db.find(selector, **kwargs)`
```python
data = db.find(selector, limit=25, skip=None, sort=None, fields=None,
               use_index=None, bookmark=None, update=True, conflicts=False)
```
Selects documents according to the Mango index `selector`.

- `selector`: The Mango index. For more information on selector syntax,
   see https://docs.couchdb.org/en/latest/api/database/find.html#find-selectors
- `limit`: Maximum number of results returned.
- `skip`: Skip the given number of results.
- `sort`: A list of dictionaries specifying the order of the results,
  where the field name is the key and the direction is the value;
  either `"asc"` or `"desc"`.
- `fields`: List specifying which fields of each result document should
  be returned. If omitted, return the entire document.
- `use_index`: String or list of strings specifying the index(es) to use.
- `bookmark`: A string that marks the end the previous set of results.
  If given, the next set of results will be returned.
- `update`: Whether to update the index prior to returning the result.
- `conflicts`: Whether to include conflicted documents.

Returns a dictionary with items `docs`, `warning`, `execution_stats`
and `bookmark`.

*CouchDB version >= 2.0*

### `db.explain(selector, **kwargs)`
```python
data = db.explain(selector, use_index=None, limit=None, skip=None,
                  sort=None, fields=None, bookmark=None)
```
Returns info on which index is being used by the query.

- `selector`: The Mango index. For more information on selector syntax,
   see https://docs.couchdb.org/en/latest/api/database/find.html#find-selectors
- `limit`: Maximum number of results returned.
- `skip`: Skip the given number of results.
- `sort`: A list of dictionaries specifying the order of the results,
  where the field name is the key and the direction is the value;
  either `"asc"` or `"desc"`.
- `fields`: List specifying which fields of each result document should
  be returned. If omitted, return the entire document.
- `bookmark`: A string that marks the end the previous set of results.
   If given, the next set of results will be returned.

*CouchDB version >= 2.0*

### `db.get_attachment(doc, filename)`

Returns a file-like object containing the content of the specified attachment.

### `db.put_attachment(doc, content, filename=None, content_type=None)`

Adds or updates the given file as an attachment to the given document
in the database.

- `content` is a string or a file-like object.
- If `filename` is not provided, then an attempt is made to get it from
  the `content` object. If this fails, `ValueError` is raised.
- If `content_type` is not provided, then an attempt to guess it from
  the filename extension is made. If that does not work, it is
  set to `"application/octet-stream"`

Returns the new revision of the document, in addition to updating
the `_rev` field in the document.

### `db.delete_attachment(doc, filename)`

Deletes the attachment.

Returns the new revision of the document, in addition to updating
the `_rev` field in the document.

### `db.dump(filepath, callback=None, exclude_designs=False, progressbar=False)`

Dumps the entire database to a `tar` file.

Returns a tuple `(ndocs, nfiles)` giving the number of documents
and attached files written out.

If defined, the function `callback(ndocs, nfiles)` is called 
every 100 documents.

If `exclude_designs` is True, design document will be excluded from the dump.

If `progressbar` is True, display a progress bar.

If the filepath ends with `.gz`, then the tar file is gzip compressed.
The `_rev` item of each document is included in the dump.

### `db.undump(filepath, callback=None, progressbar=False)`

Loads the `tar` file given by the path. It must have been produced by `db.dump`.

Returns a tuple `(ndocs, nfiles)` giving the number of documents
and attached files read from the file.

If defined, the function `callback(ndocs, nfiles)` is called 
every 100 documents.

If `progressbar` is True, display a progress bar.

**NOTE**: The documents are just added to the database, ignoring any
`_rev` items. This means that no document with the same identifier
may exist in the database.

## `CouchDB2Exception`

The Base CouchDB2 exception, from which all others derive.

## `NotFoundError`

No such entity (e.g. database or document) exists.

## `BadRequestError`

Invalid request; bad name, body or headers.

## `CreationError`

Could not create the entity; it exists already.

## `RevisionError`

Wrong or missing `_rev` item in the document to save.

## `AuthorizationError`

Current user not authorized to perform the operation.

## `ContentTypeError`

Bad `Content-Type` value in the request.

## `ServerError`

Internal CouchDB server error.

## `class ViewResult(rows, offset, total_rows)`

An instance of this class is returned as result from `db.view()`.
Instances of this class are not supposed to be created by client software.

Attributes:

- `rows`: the list of `Row` objects.
- `offset`: the offset used for the set of rows.
- `total_rows`: the total number of rows selected.

### `__len__`
```python
len(viewresult)
```
Return the number of rows in the view result.

### `__iter__`
```python
for row in viewresult: ...
```
Return an iterator over all rows in the view result.

### `__getitem__`
```python
row = viewresult[i]
```
Return the indexed view result row.

### `vr.json()`

Return the view result data in a JSON-like representation.

## `class Row(id, key, value, doc)`

Named-tuple object returned in ViewResult list attribute `rows`.

Attributes:

- `id`: the identifier of the document, if any. Alias for `row[0]`.
- `key`: the key for the index row. Alias for `row[1]`.
- `value`: the value for the index row. Alias for `row[2]`.
- `doc`: the document, if any. Alias for `row[3]`.

## Utility functions at the module level

### `read_settings(filepath, settings=None)`

Read the settings lookup from a JSON format file.

If `settings` is given, then return an updated copy of it,
else copy the default settings, update, and return it.

## Command line tool

The module is also a command line tool for interacting with the CouchDB server.
It is installed if `pip` is used to install this module.

Settings for the command line tool are updated from the following sources,
each in the order given and if it exists:

1) Default values are
   ```
   {
     "SERVER": "http://localhost:5984",
     "DATABASE": null,
     "USERNAME": null,
     "PASSWORD": null
   }
   ```
2) Read from the JSON file `~/.couchdb2` (in your home directory).
3) Read from the JSON file `settings.json` (in the current working directory).
4) Read from the JSON file given by command line option `--settings FILEPATH`.
5) From environment variables.
6) From command line arguments.

### Options

To show available command options:

```
$ couchdb2 -h
usage: couchdb2 [options]

CouchDB v2.x command line tool, leveraging Python module CouchDB2.

optional arguments:
  -h, --help            show this help message and exit
  --settings FILEPATH   Settings file in JSON format.
  -S SERVER, --server SERVER
                        CouchDB server URL, including port number.
  -d DATABASE, --database DATABASE
                        Database to operate on.
  -u USERNAME, --username USERNAME
                        CouchDB user account name.
  -p PASSWORD, --password PASSWORD
                        CouchDB user account password.
  -q, --password_question
                        Ask for the password by interactive input.
  --ca_file FILE_OR_DIRPATH
                        File or directory containing CAs.
  -o FILEPATH, --output FILEPATH
                        Write output to the given file (JSON format).
  --indent INT          Indentation level for JSON format output file.
  -y, --yes             Do not ask for confirmation (delete, destroy, undump).
  -v, --verbose         Print more information.
  -s, --silent          Print no information.

server operations:
  -V, --version         Output CouchDB server version.
  --list                Output a list of the databases on the server.

Database operations.:
  --create              Create the database.
  --destroy             Delete the database and all its contents.
  --compact             Compact the database; may take some time.
  --compact_design DDOC
                        Compact the view indexes for the named design doc.
  --view_cleanup        Remove view index files no longer required.
  --info                Output information about the database.
  --security            Output security information for the database.
  --set_security FILEPATH
                        Set security information for the database from the
                        JSON file.
  --list_designs        List design documents for the database.
  --design DDOC         Output the named design document.
  --put_design DDOC FILEPATH
                        Store the named design document from the file.
  --delete_design DDOC  Delete the named design document.
  --dump FILEPATH       Create a dump file of the database.
  --undump FILEPATH     Load a dump file into the database.

Document operations.:
  -G DOCID, --get DOCID
                        Output the document with the given identifier.
  -P FILEPATH, --put FILEPATH
                        Store the document; arg is literal doc or filepath.
  --delete DOCID        Delete the document with the given identifier.

attachments to document:
  --attach DOCID FILEPATH
                        Attach the specified file to the given document.
  --detach DOCID FILENAME
                        Remove the attached file from the given document.
  --get_attach DOCID FILENAME
                        Get the attached file from the given document; write
                        to same filepath or that given by '-o'.

query a design view, returning rows:
  --view SPEC           Design view '{design}/{view}' to query.
  --key KEY             Key value selecting view rows.
  --startkey KEY        Start key value selecting range of view rows.
  --endkey KEY          End key value selecting range of view rows.
  --startkey_docid DOCID
                        Return rows starting with the specified document.
  --endkey_docid DOCID  Stop returning rows when specified document reached.
  --group               Group the results using the 'reduce' function.
  --group_level INT     Specify the group level to use.
  --noreduce            Do not use the 'reduce' function of the view.
  --limit INT           Limit the number of returned rows.
  --skip INT            Skip this number of rows before returning result.
  --descending          Sort rows in descending order (swap start/end keys!).
  --include_docs        Include documents in result.
```
