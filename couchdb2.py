# -*- coding: utf-8 -*-
"""CouchDB v2.x Python interface in a single module. Also a command line tool.

Most, but not all, features of this module work with CouchDB version < 2.0.

Relies on requests: http://docs.python-requests.org/en/master/
"""

from __future__ import print_function

__version__ = '1.7.2'

# Standard packages
import argparse
import collections
from collections import OrderedDict as odict # Required before Python 3.7
import getpass
import gzip
import io
import json
import mimetypes
import os.path
import sys
import tarfile
import time
import uuid

# Third-party package.
import requests

JSON_MIME = 'application/json'
BIN_MIME  = 'application/octet-stream'


class Server(object):
    "A connection to the CouchDB server."

    def __init__(self, href='http://localhost:5984/',
                 username=None, password=None, use_session=True):
        """Connect to the CouchDB server.

        If `use_session` is true, then an authenticated CouchDB session
        is used transparently.

        Otherwise, username and password is sent with each request.
        """
        self.href = href.rstrip('/') + '/'
        self._session = requests.Session()
        self._session.headers.update({'Accept': JSON_MIME})
        if username and password:
            if use_session:
                self._POST('_session', 
                           data={'name': username, 'password': password})
            else:
                self._session.auth = (username, password)

    @property
    def version(self):
        try:
            return self._version
        except AttributeError:
            self._version = self._GET().json()['version']
            return self._version

    @property
    def user_context(self):
        try:
            return self._user_context
        except AttributeError:
            self._user_context = jsonod(self._GET('_session'))
            return self._user_context

    def __str__(self):
        "Return a simple string representation of the server interface."
        return "CouchDB {s.version} {s.href}".format(s=self)

    def __len__(self):
        "Return the number of user-defined databases."
        data = self._GET('_all_dbs').json()
        return len([n for n in data if not n.startswith('_')])

    def __iter__(self):
        "Return an iterator over all user-defined databases on the server."
        data = self._GET('_all_dbs').json()
        return iter([Database(self, n, check=False) 
                     for n in data if not n.startswith('_')])

    def __getitem__(self, name):
        "Get the named database."
        return Database(self, name, check=True)

    def __contains__(self, name):
        "Does the named database exist?"
        response = self._HEAD(name, errors={404: None})
        return response.status_code == 200

    def __call__(self):
        "Return meta information about the instance."
        return self._GET().json(object_pairs_hook=odict)

    def up(self):
        """Is the server up and running, ready to respond to requests?

        CouchDB version >= 2.0.
        """
        assert self.version >= '2.0'
        response = self._session.get(self.href + '_up')
        return response.status_code == 200

    def get(self, name, check=True):
        "Get the named database."
        return Database(self, name, check=check)

    def create(self, name):
        "Create the named database."
        return Database(self, name, check=False).create()

    def get_config(self, nodename='_local'):
        """Get the named node's configuration.

        CouchDB version >= 2.0.
        """
        assert self.version >= '2.0'
        return jsonod(self._GET('_node', nodename, '_config'))

    def get_active_tasks(self):
        "Return a list of running tasks."
        return jsonod(self._GET('_active_tasks'))

    def get_cluster_setup(self, ensure_dbs_exists=None):
        """Return the status of the node or cluster.

        CouchDB version >= 2.0.
        """
        assert self.version >= '2.0'
        if ensure_dbs_exists is None:
            params = {}
        else:
            params = {'ensure_dbs_exists': ensure_dbs_exists}
        return jsonod(self._GET('_cluster_setup', params=params))

    def set_cluster_setup(self, doc):
        """Configure a node as a single node, as part of a cluster,
        or finalize a cluster.

        CouchDB version >= 2.0.
        """
        assert self.version >= '2.0'
        self._POST('_cluster_setup', json=doc)

    def get_membership(self):
        """Return data about the nodes that are part of the cluster.

        CouchDB version >= 2.0.
        """
        assert self.version >= '2.0'
        return jsonod(self._GET('_membership'))

    def set_replicate(self, doc):
        "Request, configure, or stop, a replication operation."
        return self._POST('_replicate', json=doc)

    def get_scheduler_jobs(self, limit=None, skip=None):
        """Get a list of replication jobs.

        CouchDB version >= 2.0.
        """
        assert self.version >= '2.0'
        params = {}
        if limit is not None:
            params['limit'] = jsons(limit)
        if skip is not None:
            params['skip'] = jsons(skip)
        return jsonod(self._GET('_scheduler/jobs', params=params))

    def get_scheduler_docs(self, limit=None, skip=None,
                           replicator_db=None, docid=None):
        """Get information about replication document(s).

        CouchDB version >= 2.0.
        """
        assert self.version >= '2.0'
        params = {}
        if limit is not None:
            params['limit'] = jsons(limit)
        if skip is not None:
            params['skip'] = jsons(skip)
        args = ['_scheduler/docs']
        if replicator_db is not None:
            args.append(replicator_db)
            if docid is not None:
                args.append(docid)
        return jsonod(self._GET(*args, params=params))

    def get_node_stats(self, nodename='_local'):
        """"Return statistics for the running server.

        CouchDB version >= 2.0.
        """
        assert self.version >= '2.0'
        return jsonod(self._GET('_node', nodename, '_stats'))

    def get_node_system(self, nodename='_local'):
        """Return various system-level statistics for the running server.

        CouchDB version >= 2.0.
        """
        assert self.version >= '2.0'
        return jsonod(self._GET('_node', nodename, '_system'))

    def _HEAD(self, *segments, **kwargs):
        "HTTP HEAD request to the CouchDB server."
        response = self._session.head(self._href(segments))
        self._check(response, errors=kwargs.get('errors', {}))
        return response

    def _GET(self, *segments, **kwargs):
        "HTTP GET request to the CouchDB server."
        kw = self._kwargs(kwargs, 'headers', 'params')
        response = self._session.get(self._href(segments), **kw)
        self._check(response, errors=kwargs.get('errors', {}))
        return response

    def _PUT(self, *segments, **kwargs):
        "HTTP PUT request to the CouchDB server."
        kw = self._kwargs(kwargs, 'json', 'data', 'headers')
        response = self._session.put(self._href(segments), **kw)
        self._check(response, errors=kwargs.get('errors', {}))
        return response

    def _POST(self, *segments, **kwargs):
        "HTTP POST request to the CouchDB server."
        kw = self._kwargs(kwargs, 'json', 'data', 'headers', 'params')
        response = self._session.post(self._href(segments), **kw)
        self._check(response, errors=kwargs.get('errors', {}))
        return response

    def _DELETE(self, *segments, **kwargs):
        """HTTP DELETE request to the CouchDB server.
        Pass parameters in the keyword argument 'params'.
        """
        kw = self._kwargs(kwargs, 'headers')
        response = self._session.delete(self._href(segments), **kw)
        self._check(response, errors=kwargs.get('errors', {}))
        return response

    def _href(self, segments):
        "Return the complete URL."
        return self.href + '/'.join(segments)

    def _kwargs(self, kwargs, *keys):
        "Return the kwargs for the specified keys."
        result = {}
        for key in keys:
            try:
                result[key] = kwargs[key]
            except KeyError:
                pass
        return result

    def _check(self, response, errors={}):
        "Raise an exception if the response status code indicates an error."
        try:
            error = errors[response.status_code]
        except KeyError:
            try:
                error = _ERRORS[response.status_code]
            except KeyError:
                raise IOError("{r.status_code} {r.reason}".format(r=response))
        if error is not None:
            raise error(response.reason)


class Database(object):
    "Interface to a named CouchDB database."

    CHUNK_SIZE = 100

    def __init__(self, server, name, check=True):
        self.server = server
        self.name = name
        if check:
            self.check()

    def __str__(self):
        "Return the name of the CouchDB database."
        return self.name

    def __len__(self):
        "Return the number of documents in the database."
        return self.server._GET(self.name).json()['doc_count']

    def __contains__(self, id):
        "Does a document with the given id exist in the database?"
        response = self.server._HEAD(self.name, id, errors={404: None})
        return response.status_code in (200, 304)

    def __iter__(self):
        "Return an iterator over all documents in the database."
        return _DatabaseIterator(self, chunk_size=self.CHUNK_SIZE)

    def __getitem__(self, id):
        "Return the document with the given id."
        result = self.get(id)
        if result is None:
            raise NotFoundError('no such document')
        else:
            return result

    def exists(self):
        "Does the database exist?"
        response = self.server._HEAD(self.name, errors={404: None})
        return response.status_code == 200

    def check(self):
        "Raises NotFoundError if the database does not exist."
        if not self.exists():
            raise NotFoundError("database '{}' does not exist".format(self))

    def create(self):
        "Create the database."
        self.server._PUT(self.name)
        return self

    def destroy(self):
        "Delete the database and all its contents."
        self.server._DELETE(self.name)

    def get_info(self):
        "Return a dictionary with information about the database."
        return jsonod(self.server._GET(self.name))

    def get_security(self):
        "Return a dictionary with security information for the database."
        return jsonod(self.server._GET(self.name, '_security'))

    def set_security(self, doc):
        "Set the security information for the database."
        self.server._PUT(self.name, '_security', json=doc)

    def compact(self, finish=False, callback=None):
        """Compact the CouchDB database by rewriting the disk database file
        and removing old revisions of documents.

        If `finish` is True, then return only when compaction is done.
        In addition, if defined, the function `callback(seconds)` is called
        every second until compaction is done.
        """
        self.server._POST(self.name, '_compact',
                          headers={'Content-Type': JSON_MIME})
        if finish:
            response = self.server._GET(self.name)
            seconds = 0
            while response.json().get('compact_running'):
                time.sleep(1)
                seconds += 1
                if callback: callback(seconds)
                response = self.server._GET(self.name)

    def compact_design(self, designname):
        "Compact the view indexes associated with the named design document."
        self.server._POST(self.name, '_compact', designname,
                          headers={'Content-Type': JSON_MIME})

    def view_cleanup(self):
        """Remove unnecessary view index files due to changed views in
        design documents of the database.
        """
        self.server._POST(self.name, '_view_cleanup')

    def get(self, id, rev=None, revs_info=False, default=None):
        """Return the document with the given id,
        or the `default` value if not found.
        """
        params = {}
        if rev is not None:
            params['rev'] = rev
        if revs_info:
            params['revs_info'] = jsons(True)
        response = self.server._GET(self.name, id,
                                    errors={404: None}, params=params)
        if response.status_code == 404:
            return default
        return jsonod(response)

    def put(self, doc):
        """Insert or update the document.

        If the document is already in the database, the `_rev` item must
        be present in the document, and it will be updated.

        If the document does not contain an item `_id`, it is added
        having a UUID4 value. The `_rev` item is also added.
        """
        if '_id' not in doc:
            doc['_id'] = uuid.uuid4().hex
        response = self.server._PUT(self.name, doc['_id'], json=doc)
        doc['_rev'] = response.json()['rev']

    def delete(self, doc):
        "Delete the document."
        if '_id' not in doc:
            raise NotFoundError("missing '_id' item in the document")
        if '_rev' not in doc:
            raise RevisionError("missing '_rev' item in the document")
        response = self.server._DELETE(self.name, doc['_id'], 
                                       headers={'If-Match': doc['_rev']})

    def get_designs(self):
        """Return the design documents for the database.

        CouchDB version >= 2.2.
        """
        assert self.server.version >= '2.2'
        return jsonod(self.server._GET(self.name, '_design_docs'))

    def get_design(self, designname):
        "Get the named design document."
        return jsonod(self.server._GET(self.name, '_design', designname))

    def put_design(self, designname, doc, rebuild=True):
        """Insert or update the design document under the given name.

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
        """
        response = self.server._GET(self.name, '_design', designname,
                                    errors={404: None})
        if response.status_code == 200:
            current_doc = response.json(object_pairs_hook=odict)
            doc['_id'] = current_doc['_id']
            doc['_rev'] = current_doc['_rev']
            if doc == current_doc: 
                return False
        response = self.server._PUT(self.name, '_design', designname, json=doc)
        if rebuild:
            for view in doc.get('views', {}):
                self.view(designname, view, limit=1)
        return True

    def view(self, designname, viewname, key=None, keys=None,
             startkey=None, endkey=None,
             skip=None, limit=None, sorted=True, descending=False,
             group=False, group_level=None, reduce=None,
             include_docs=False):
        """Return a ViewResult object, containing the following attributes:
        - `rows`: the list of Row objects.
        - `offset`: the offset used for the set of rows.
        - `total_rows`: the total number of rows selected.

        A Row object contains the following attributes:
        - `id`: the identifier of the document, if any.
        - `key`: the key for the index row.
        - `value`: the value for the index row.
        - `doc`: the document, if any.
        """
        params = {}
        if startkey is not None:
            params['startkey'] = jsons(startkey)
        if key is not None:
            params['key'] = jsons(key)
        if keys is not None:
            params['keys'] = jsons(keys)
        if endkey is not None:
            params['endkey'] = jsons(endkey)
        if skip is not None:
            params['skip'] = jsons(skip)
        if limit is not None:
            params['limit'] = jsons(limit)
        if not sorted:
            params['sorted'] = jsons(False)
        if descending:
            params['descending'] = jsons(True)
        if group:
            params['group'] = jsons(True)
        if group_level is not None:
            params['group_level'] = jsons(group_level)
        if reduce is not None:
            params['reduce'] = jsons(bool(reduce))
        if include_docs:
            params['include_docs'] = jsons(True)
        response = self.server._GET(self.name, '_design', designname, '_view',
                                    viewname, params=params)
        data = response.json()
        return ViewResult([Row(r.get('id'),
                               r.get('key'),
                               r.get('value'),
                               r.get('doc')) 
                           for r in data.get('rows', [])],
                          data.get('offset'),
                          data.get('total_rows'))

    def get_indexes(self):
        """Return a list of all indexes in the database.

        CouchDB version >= 2.0.
        """
        assert self.server.version >= '2.0'
        return jsonod(self.server._GET(self.name, '_index'))

    def put_index(self, fields, ddoc=None, name=None, selector=None):
        """Store a Mango index specification.

        - `fields` is a list of fields to index.
        - `ddoc` is the design document name. Generated if none given.
        - `name` is the name of the index. Generated if none given.
        - `selector` is a partial filter selector, which may be omitted.

        Returns a dictionary with items `id` (design document identifier; sic!),
        `name` (index name) and `result` (`created` or `exists`).

        CouchDB version >= 2.0.
        """
        assert self.server.version >= '2.0'
        doc = {'index': {'fields': fields}}
        if ddoc is not None:
            doc['ddoc'] = ddoc
        if name is not None:
            doc['name'] = name
        if selector is not None:
            doc['index']['partial_filter_selector'] = selector
        return jsonod(self.server._POST(self.name, '_index', json=doc))

    def delete_index(ddoc, name):
        """Delete the named index.

        CouchDB version >= 2.0.
        """
        assert self.server.version >= '2.0'
        self.server._DELETE(self.name, '_index', ddoc, 'json', name)

    def find(self, selector, limit=None, skip=None, sort=None, fields=None,
             use_index=None, bookmark=None, update=None):
        """Select documents according to the selector.

        Returns a dictionary with items `docs`, `warning`, `execution_stats`
        and `bookmark`.

        CouchDB version >= 2.0.
        """
        assert self.server.version >= '2.0'
        doc = {'selector': selector}
        if limit is not None:
            doc['limit'] = limit
        if skip is not None:
            doc['skip'] = skip
        if sort is not None:
            doc['sort'] = sort
        if fields is not None:
            doc['fields'] = fields
        if use_index is not None:
            doc['use_index'] = use_index
        if bookmark is not None:
            doc['bookmark'] = bookmark
        if update is not None:
            doc['update'] = update
        return jsonod(self.server._POST(self.name, '_find', json=doc))

    def explain(self, selector, limit=None, skip=None, sort=None, fields=None,
             use_index=None, bookmark=None, update=None):
        """Return info on which index is being used by the query.

        CouchDB version >= 2.0.
        """
        assert self.server.version >= '2.0'
        doc = {'selector': selector}
        if limit is not None:
            doc['limit'] = limit
        if skip is not None:
            doc['skip'] = skip
        if sort is not None:
            doc['sort'] = sort
        if fields is not None:
            doc['fields'] = fields
        if use_index is not None:
            doc['use_index'] = use_index
        if bookmark is not None:
            doc['bookmark'] = bookmark
        if update is not None:
            doc['update'] = update
        return jsonod(self.server._POST(self.name, '_explain', json=doc))

    def get_attachment(self, doc, filename):
        "Return a file-like object containing the content of the attachment."
        response = self.server._GET(self.name, doc['_id'], filename,
                                    headers={'If-Match': doc['_rev']})
        return io.BytesIO(response.content)

    def put_attachment(self, doc, content, filename=None, content_type=None):
        """`content` is a string or a file-like object. Return the new
        revision of the document.

        If no filename, then an attempt is made to get it from content object.
        """
        if filename is None:
            try:
                filename = content.name
            except AttributeError:
                raise ValueError('could not figure out filename')
        if not content_type:
            (content_type, enc) = mimetypes.guess_type(filename, strict=False)
            if not content_type: content_type = BIN_MIME
        response = self.server._PUT(self.name, doc['_id'], filename,
                                    data=content,
                                    headers={'Content-Type': content_type,
                                             'If-Match': doc['_rev']})
        return response.json()['rev']

    def delete_attachment(self, doc, filename):
        "Delete the attachment. Return the new revision of the document."
        response = self.server._DELETE(self.name, doc['_id'], filename,
                                       headers={'If-Match': doc['_rev']})
        return response.json()['rev']

    def dump(self, filepath, callback=None):
        """Dump the entire database to a tar file.

        If defined, the function `callback(ndocs, nfiles)` is called 
        every 100 documents.

        If the filepath ends with `.gz`, then the tar file is gzip compressed.
        The `_rev` item of each document is kept.

        A tuple (ndocs, nfiles) is returned.
        """
        ndocs = 0
        nfiles = 0
        if filepath.endswith('.gz'):
            mode = 'w:gz'
        else:
            mode = 'w'
        with tarfile.open(filepath, mode=mode) as outfile:
            for doc in self:
                info = tarfile.TarInfo(doc['_id'])
                data = jsons(doc).encode('utf-8')
                info.size = len(data)
                outfile.addfile(info, io.BytesIO(data))
                ndocs += 1
                # Attachments must follow their document.
                for attname in doc.get('_attachments', dict()):
                    info = tarfile.TarInfo(u"{0}_att/{1}".format(
                        doc['_id'], attname))
                    attfile = self.get_attachment(doc, attname)
                    if attfile is None:
                        attdata = ''
                    else:
                        attdata = attfile.read()
                        attfile.close()
                    info.size = len(attdata)
                    outfile.addfile(info, io.BytesIO(attdata))
                    nfiles += 1
                if ndocs % 100 == 0 and callback:
                    callback(ndocs, nfiles)
        return (ndocs, nfiles)

    def undump(self, filepath, callback=None):
        """Load the named tar file, which must have been produced by `dump`.

        If defined, the function `callback(ndocs, nfiles)` is called 
        every 100 documents.

        NOTE: The documents are just added to the database, ignoring any
        `_rev` items. This means that no document with the same identifier
        must exist in the database.

        A tuple (ndocs, nfiles) is returned.
        """
        ndocs = 0
        nfiles = 0
        atts = dict()
        with tarfile.open(filepath, mode='r') as infile:
            for item in infile:
                itemfile = infile.extractfile(item)
                itemdata = itemfile.read()
                itemfile.close()
                if item.name in atts:
                    # An attachment follows its document.
                    self.put_attachment(doc, itemdata, **atts.pop(item.name))
                    nfiles += 1
                else:
                    doc = json.loads(itemdata.decode('utf-8'),
                                     object_pairs_hook=odict)
                    doc.pop('_rev', None)
                    atts = doc.pop('_attachments', dict())
                    self.put(doc)
                    ndocs += 1
                    for attname, attinfo in atts.items():
                        key = u"{0}_att/{1}".format(doc['_id'], attname)
                        atts[key] = dict(filename=attname,
                                         content_type=attinfo['content_type'])
                if ndocs % 100 == 0 and callback:
                    callback(ndocs, nfiles)
        return (ndocs, nfiles)


class _DatabaseIterator(object):
    "Iterator over all documents in a database."

    def __init__(self, db, chunk_size):
        self.db = db
        self.skip = 0
        self.chunk = []
        self.chunk_size = chunk_size

    def __next__(self):
        return self.next()

    def next(self):
        try:
            return self.chunk.pop()
        except IndexError:
            response = self.db.server._GET(self.db.name, '_all_docs',
                                           params={'include_docs': True,
                                                   'skip': self.skip,
                                                   'limit': self.chunk_size})
            data = response.json(object_pairs_hook=odict)
            rows = data['rows']
            if len(rows) == 0:
                raise StopIteration
            self.chunk = [r['doc'] for r in rows]
            self.chunk.reverse()
            self.skip = data['offset'] + len(self.chunk)
            return self.chunk.pop()


class ViewResult(object):
    "Result of view query; contains rows, offset, total_rows."

    def __init__(self, rows, offset, total_rows):
        self.rows = rows
        self.offset = offset
        self.total_rows = total_rows

    def __len__(self):
        return len(self.rows)

    def __getitem__(self, i):
        return self.rows[i]

    def __iter__(self):
        return iter(self.rows)

    def json(self):
        "Return data in a JSON-like representation."
        result = odict()
        result['total_rows'] = self.total_rows
        result['offset'] = self.offset
        result['rows'] = [r._asdict() for r in self]
        return result

Row = collections.namedtuple('Row', ['id', 'key', 'value', 'doc'])

class CouchDB2Exception(Exception):
    "Base CouchDB2 exception."

class NotFoundError(CouchDB2Exception):
    "No such entity exists."

class BadRequestError(CouchDB2Exception):
    "Invalid request; bad name, body or headers."

class CreationError(CouchDB2Exception):
    "Could not create the entity; it exists already."

class RevisionError(CouchDB2Exception):
    "Wrong or missing '_rev' item in the document to put."

class AuthorizationError(CouchDB2Exception):
    "Current user not authorized to perform the operation."

class ContentTypeError(CouchDB2Exception):
    "Bad 'Content-Type' value in the request."

class ServerError(CouchDB2Exception):
    "Internal server error."

_ERRORS = {
    200: None,
    201: None,
    202: None,
    304: None,
    400: BadRequestError,
    401: AuthorizationError,
    403: AuthorizationError,
    404: NotFoundError,
    409: RevisionError,
    412: CreationError,
    415: ContentTypeError,
    500: ServerError}

def jsons(data, indent=None):
    "Convert data into JSON string."
    return json.dumps(data, ensure_ascii=False, indent=indent)

def jsonod(response):
    "Return JSON using ordered dictionary."
    return response.json(object_pairs_hook=odict)

def get_parser():
    "Get the parser for the command line tool."
    p = argparse.ArgumentParser(prog='couchdb2',
                                usage='%(prog)s [options]',
                                description='CouchDB v2.x command line tool,'
                                ' leveraging Python module CouchDB2.')
    p.add_argument('--settings', metavar='FILEPATH',
                   help='settings file in JSON format')
    p.add_argument('-S', '--server',
                   help='CouchDB server URL, including port number')
    p.add_argument('-d', '--database', help='database to operate on')
    p.add_argument('-u', '--username', help='CouchDB user account name')
    x01 = p.add_mutually_exclusive_group()
    x01.add_argument('-p', '--password', help='CouchDB user account password')
    x01.add_argument('-q', '--password_question', action='store_true',
                     help='ask for the password by interactive input')
    p.add_argument('-o', '--output', metavar='FILEPATH',
                   help='write output to the given file (JSON format)')
    p.add_argument('--indent', type=int, metavar='INT',
                   help='indentation level for JSON format output file')
    p.add_argument('-y', '--yes', action='store_true',
                   help='do not ask for confirmation (delete, destroy)')
    x02 = p.add_mutually_exclusive_group()
    x02.add_argument('-v', '--verbose', action='store_true',
                     help='print more information')
    x02.add_argument('-s', '--silent', action='store_true',
                     help='print no information')

    g0 = p.add_argument_group('server operations')
    g0.add_argument('-V', '--version', action='store_true',
                    help='output CouchDB server version')
    g0.add_argument('--list', action='store_true',
                    help='output a list of the databases on the server')

    g1 = p.add_argument_group('database operations')
    x11 = g1.add_mutually_exclusive_group()
    x11.add_argument('--create', action='store_true',
                     help='create the database')
    x11.add_argument('--destroy', action='store_true',
                     help='delete the database and all its contents')
    g1.add_argument('--compact', action='store_true',
                    help='compact the database; may take some time')
    g1.add_argument('--compact_design', metavar='DDOC',
                    help='compact the view indexes for the named design doc')
    g1.add_argument('--view_cleanup', action='store_true',
                    help='remove view index files no longer required')
    g1.add_argument('--info', action='store_true',
                     help='output information about the database')
    x12 = g1.add_mutually_exclusive_group()
    x12.add_argument('--security', action='store_true',
                     help='output security information for the database')
    x12.add_argument('--set_security', metavar='FILEPATH',
                     help='set security information for the database'
                     ' from the JSON file')
    x13 = g1.add_mutually_exclusive_group()
    x13.add_argument('--list_designs', action='store_true',
                     help='list design documents for the database')
    x13.add_argument('--design', metavar='DDOC',
                     help='output the named design document')
    x13.add_argument('--put_design', nargs=2, metavar=('DDOC', 'FILEPATH'),
                     help='store the named design document from the file')
    x13.add_argument('--delete_design', metavar='DDOC',
                     help='delete the named design document')
    x14 = g1.add_mutually_exclusive_group()
    x14.add_argument('--dump', metavar='FILEPATH',
                     help='create a dump file of the database')
    x14.add_argument('--undump', metavar='FILEPATH',
                     help='load a dump file into the database')

    g2 = p.add_argument_group('document operations')
    x2 = g2.add_mutually_exclusive_group()
    x2.add_argument('-G', '--get', metavar="DOCID",
                    help='output the document with the given identifier')
    x2.add_argument('-P', '--put', metavar='FILEPATH',
                    help='store the document; arg is literal doc or filepath')
    x2.add_argument('--delete', metavar="DOCID",
                    help='delete the document with the given identifier')

    g3 = p.add_argument_group('attachments to document')
    x3 = g3.add_mutually_exclusive_group()
    x3.add_argument('--attach', nargs=2, metavar=('DOCID', 'FILEPATH'),
                    help='attach the specified file to the given document')
    x3.add_argument('--detach', nargs=2, metavar=('DOCID', 'FILENAME'),
                    help='remove the attached file from the given document')
    x3.add_argument('--get_attach', nargs=2, metavar=('DOCID', 'FILENAME'),
                    help='get the attached file from the given document;'
                    " write to same filepath or that given by '-o'")

    g4 = p.add_argument_group('query a design view, returning rows')
    g4.add_argument('--view', metavar="SPEC",
                    help="design view '{design}/{view}' to query")
    x41 = g4.add_mutually_exclusive_group()
    x41.add_argument('--key', metavar="KEY",
                    help="key value selecting view rows")
    x41.add_argument('--startkey', metavar="KEY",
                    help="start key value selecting range of view rows")
    g4.add_argument('--endkey', metavar="KEY",
                    help="end key value selecting range of view rows")
    g4.add_argument('--startkey_docid', metavar="DOCID",
                    help='return rows starting with the specified document')
    g4.add_argument('--endkey_docid', metavar="DOCID",
                    help='stop returning rows when specified document reached')
    g4.add_argument('--group', action='store_true',
                     help="group the results using the 'reduce' function")
    g4.add_argument('--group_level', type=int, metavar='INT',
                     help='specify the group level to use')
    g4.add_argument('--noreduce', action='store_true',
                     help="do not use the 'reduce' function of the view")
    g4.add_argument('--limit', type=int, metavar='INT',
                    help='limit the number of returned rows')
    g4.add_argument('--skip', type=int, metavar='INT',
                    help='skip this number of rows before returning result')
    g4.add_argument('--descending', action='store_true',
                    help='sort rows in descending order (swap start/end keys!)')
    g4.add_argument('--include_docs', action='store_true',
                    help='include documents in result')
    return p


def get_settings(pargs):
    """Get the settings lookup for the command line tool.
    1) Initialize with DEFAULT_SETTINGS
    2) Update with values in JSON file in DEFAULT_SETTINGS_FILEPATHS, if any.
    3) Update with values in the explicitly given settings file, if any.
    4) Modify by any command line arguments.
    """
    settings = None
    filepaths = DEFAULT_SETTINGS_FILEPATHS[:]
    if pargs.settings:
        filepaths.append(pargs.settings)
    for filepath in filepaths:
        try:
            settings = read_settings(filepath, settings=settings)
            verbose(pargs, 'settings read from file', filepath)
        except IOError:
            verbose(pargs, 'Warning: no settings file', filepath)
        except (ValueError, TypeError):
            sys.exit('Error: bad settings file', filepath)
    if pargs.server:
        settings['SERVER'] = pargs.server
    if pargs.database:
        settings['DATABASE'] = pargs.database
    if pargs.username:
        settings['USERNAME'] = pargs.username
    if pargs.password:
        settings['PASSWORD'] = pargs.password
    if pargs.verbose:
        s = odict()
        for key in ['SERVER', 'DATABASE', 'USERNAME']:
            s[key] = settings[key]
        if settings['PASSWORD'] is None:
            s['PASSWORD'] = None
        else:
            s['PASSWORD'] = '***'
        verbose(pargs, 'settings:', jsons(s))
    return settings

DEFAULT_SETTINGS = {
    'SERVER': 'http://localhost:5984',
    'DATABASE': None,
    'USERNAME': None,
    'PASSWORD': None
}

DEFAULT_SETTINGS_FILEPATHS = ['~/.couchdb2', 'settings.json']

def read_settings(filepath, settings=None):
    """Read the settings lookup from a JSON format file.
    If `settings` is given, then return an updated copy of it,
    else copy the default settings, update, and return.
    """
    if settings:
        result = settings.copy()
    else:
        result = DEFAULT_SETTINGS.copy()
    with open(os.path.expanduser(filepath), 'rb') as infile:
        data = json.load(infile)
        for key in DEFAULT_SETTINGS:
            for prefix in ['', 'COUCHDB_', 'COUCHDB2_']:
                try:
                    result[key] = data[prefix + key]
                except KeyError:
                    pass
    return result

def get_database(server, settings):
    "Get the database defined in the settings,"
    if not settings['DATABASE']:
        sys.exit('Error: no database defined')
    return server[settings['DATABASE']]

def message(pargs, *args):
    "Unless flag '--silent' was used, print the arguments."
    if pargs.silent: return
    print(*args)

def verbose(pargs, *args):
    "If flag '--verbose' was used, then print the arguments."
    if not pargs.verbose: return
    print(*args)

def json_output(pargs, data, else_print=False):
    """If `--output` was used, write the data in JSON format to the file.
    The indentation level is set by `--indent`.
    If the filepath ends in `.gz`. then a gzipped file is produced.

    If `--output` was not used and `else_print` is True,
    then use `print()` for indented JSON output.

    Return True if `--output` was used, else False.
    """
    if pargs.output:
        if pargs.output.endswith('.gz'):
            with gzip.open(pargs.output, 'w') as outfile:
                js = json.dumps(data, ensure_ascii=False, indent=pargs.indent)
                outfile.write(js.encode('utf-8'))
        else:
            with io.open(pargs.output, 'w', encoding='utf-8') as outfile:
                js = json.dumps(data, ensure_ascii=False, indent=pargs.indent)
                outfile.write(unicode(js))
        verbose(pargs, 'wrote JSON to file', pargs.output)
    elif else_print:
        print(jsons(data, indent=2))
    return bool(pargs.output)

def json_input(filepath):
    "Read the JSON document file."
    try:
        with open(filepath, 'r') as infile:
            return json.load(infile, object_pairs_hook=odict)
    except (IOError, ValueError, TypeError) as error:
        sys.exit("Error: {}".format(error))

def print_dot(*args):
    "Print a dot without a newline and flush immediately."
    print('.', sep='', end='')
    sys.stdout.flush()

def execute(pargs, settings):
    "Execution of the CouchDB2 command line tool."
    try:
        input = raw_input
    except NameError:
        pass
    server = Server(href=settings['SERVER'],
                    username=settings['USERNAME'],
                    password=settings['PASSWORD'])
    if pargs.verbose and server.user_context:
        print('user context:', server.user_context)
    if pargs.version:
        if not json_output(pargs, server.version):
            print(server.version)
    if pargs.list:
        dbs = list(server)
        if not json_output(pargs, [str(db) for db in dbs]):
            for db in dbs:
                print(db)

    if pargs.create:
        db = server.create(settings['DATABASE'])
        message(pargs, 'created database', db)
    elif pargs.destroy:
        db = get_database(server, settings)
        if not pargs.yes:
            answer = input("really destroy database '{}' [n] ? ".format(db))
            if answer and answer.lower()[0] in ('y', 't'):
                pargs.yes = True
        if pargs.yes:
            db.destroy()
            message(pargs, "destroyed database '{}".format(db))

    if pargs.compact:
        db = get_database(server, settings)
        if pargs.silent:
            db.compact(finish=True)
        else:
            print("compacting '{}'.".format(db), sep='', end='')
            sys.stdout.flush()
            db.compact(finish=True, callback=print_dot)
            print()
    if pargs.compact_design:
        get_database(server, settings).compact_design(pargs.compact_design)
    if pargs.view_cleanup:
        get_database(server, settings).view_cleanup()

    if pargs.info:
        db = get_database(server, settings)
        json_output(pargs, db.get_info(), else_print=True)

    if pargs.security:
        db = get_database(server, settings)
        json_output(pargs, db.get_security(), else_print=True)
    elif pargs.set_security:
        db = get_database(server, settings)
        db.set_security(json_input(pargs.set_security))

    if pargs.list_designs:
        data = get_database(server, settings).get_designs()
        if not json_output(pargs, data):
            for row in data['rows']:
                print(row['id'][len('_design/'):])
    elif pargs.design:
        db = get_database(server, settings)
        json_output(pargs, db.get_design(pargs.get_design), else_print=True)
    elif pargs.put_design:
        doc = json_input(pargs.put_design[1])
        get_database(server, settings).put_design(pargs.put_design[0], doc)
        message(pargs, 'stored design', pargs.put_design[0])
    elif pargs.delete_design:
        db = get_database(server, settings)
        doc = db.get_design(pargs.delete_design)
        db.delete(doc)
        message(pargs, 'deleted design', pargs.delete_design)

    if pargs.get:
        doc = get_database(server, settings)[pargs.get.decode('utf-8')]
        json_output(pargs, doc, else_print=True)
    elif pargs.put:
        try:                    # Attempt to interpret arg as explicit doc
            doc = json.loads(pargs.put, object_pairs_hook=odict)
        except (ValueError, TypeError): # Arg is filepath to doc
            doc = json_input(pargs.put)
        get_database(server, settings).put(doc)
        message(pargs, 'stored doc', doc['_id'])
    elif pargs.delete:
        db = get_database(server, settings)
        doc = db[pargs.delete]
        db.delete(doc)
        message(pargs, 'deleted doc', doc['_id'])

    if pargs.attach:
        db = get_database(server, settings)
        doc = db[pargs.attach[0]]
        with open(pargs.attach[1], 'rb') as infile:
            # Non-trivial decision: concluded that it is the basename of
            # the file that is the best identifier for the attachment, 
            # not the entire filepath.
            db.put_attachment(doc, infile, 
                              filename=os.path.basename(pargs.attach[1]))
        message(pargs, 
                "attached file '{1}' to doc '{0}'".format(*pargs.attach))
    elif pargs.detach:
        db = get_database(server, settings)
        doc = db[pargs.detach[0]]
        db.delete_attachment(doc, pargs.detach[1])
        message(pargs,
                "detached file '{1}' from doc '{0}'".format(*pargs.detach))
    elif pargs.get_attach:
        db = get_database(server, settings)
        doc = db[pargs.get_attach[0]]
        filepath = pargs.output or pargs.get_attach[1]
        with open(filepath, 'wb') as outfile:
            outfile.write(db.get_attachment(doc, pargs.get_attach[1]).read())
        message(pargs,
                "wrote file '{0}' from doc '{1}' attachment '{2}'".
                format(filepath, *pargs.get_attach))

    if pargs.view:
        try:
            design, view = pargs.view.split('/')
        except ValueError:
            sys.exit('Error: invalid view specification')
        kwargs = {}
        for key in ('key', 
                    'startkey', 'endkey', 'startkey_docid', 'endkey_docid',
                    'group', 'group_level', 'limit', 'skip',
                    'descending', 'include_docs'):
            value = getattr(pargs, key)
            if value is not None:
                kwargs[key] = value
        if pargs.noreduce:
            kwargs['reduce'] = False
        result = get_database(server, settings).view(design, view, **kwargs)
        json_output(pargs, result.json(), else_print=True)

    if pargs.dump:
        db = get_database(server, settings)
        if pargs.silent:
            db.dump(pargs.dump)
        else:
            print("dumping '{}'.".format(db), sep='', end='')
            sys.stdout.flush()
            ndocs, nfiles = db.dump(pargs.dump, callback=print_dot)
            print()
            print('dumped', ndocs, 'documents,', nfiles, 'files')
    elif pargs.undump:
        db = get_database(server, settings)
        if len(db) != 0:
            sys.exit("database '{}' is not empty".format(db))
        if pargs.silent:
            db.undump(pargs.undump)
        else:
            print("undumping '{}'.".format(db), sep='', end='')
            sys.stdout.flush()
            ndocs, nfiles = db.undump(pargs.undump, callback=print_dot)
            print()
            print('undumped', ndocs, 'documents,', nfiles, 'files')

def main():
    "Entry point for the CouchDB2 command line tool."
    try:
        parser = get_parser()
        pargs = parser.parse_args()
        if len(sys.argv) == 1:
            parser.print_usage()
        settings = get_settings(pargs)
        if pargs.password_question:
            settings['PASSWORD'] = getpass.getpass('password > ')
        execute(pargs, settings)
    except CouchDB2Exception as error:
        sys.exit("Error: {}".format(error))


if __name__ == '__main__':
    main()
