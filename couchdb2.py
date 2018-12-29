"""Slim Python interface module to CouchDB v2.x.

Relies on requests: http://docs.python-requests.org/en/master/
"""

from __future__ import print_function

__version__ = '1.3.2'

import argparse
import collections
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

import requests

JSON_MIME = 'application/json'
BIN_MIME  = 'application/octet-stream'


class Server(object):
    "A connection to the CouchDB server."

    def __init__(self, href='http://localhost:5984/',
                 username=None, password=None):
        """Connect to the CouchDB server.
        - Raises IOError if failure.
        """
        self.href = href.rstrip('/') + '/'
        self.username = username
        self.password = password
        self._session = requests.Session()
        if self.username and self.password:
            self._session.auth = (self.username, self.password)
        self._session.headers.update({'Accept': JSON_MIME})
        self.version = self._GET().json()['version']

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
        """Get the named database.
        - Raises NotFoundError if no such database.
        """
        return Database(self, name, check=True)

    def __contains__(self, name):
        "Does the named database exist?"
        response = self._HEAD(name, errors={404: None})
        return response.status_code == 200

    def get(self, name, check=True):
        """Get the named database.
        - Raises NotFoundError if 'check' is True and no database exists.
        """
        return Database(self, name, check=check)

    def create(self, name):
        """Create the named database.
        - Raises BadRequestError if the name is invalid.
        - Raises AuthorizationError if not server admin privileges.
        - Raises CreationError if a database with that name already exists.
        - Raises IOError if there is some other error.
        """
        return Database(self, name, check=False).create()

    def get_config(self, nodename='_local'):
        "Get the named node's configuration."
        return self._GET('_node', nodename, '_config').json()

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
        """Does a document with the given id exist in the database?
        - Raises AuthorizationError if not privileged to read.
        - Raises IOError if something else went wrong.
        """
        response = self.server._HEAD(self.name, id, errors={404: None})
        return response.status_code in (200, 304)

    def __iter__(self):
        "Iterate over all documents in the database."
        return _DatabaseIterator(self, chunk_size=self.CHUNK_SIZE)

    def __getitem__(self, id):
        """Return the document with the given id.
        - Raises AuthorizationError if not privileged to read.
        - Raises NotFoundError if no such document or database.
        - Raises IOError if something else went wrong.
        """
        result = self.get(id)
        if result is None:
            raise NotFoundError('no such document')
        else:
            return result

    def exists(self):
        "Does this database exist?"
        response = self.server._HEAD(self.name, errors={404: None})
        return response.status_code == 200

    def check(self):
        "- Raises NotFoundError if this database does not exist."
        if not self.exists():
            raise NotFoundError("database '{}' does not exist".format(self))

    def create(self):
        """Create this database.
        - Raises BadRequestError if the name is invalid.
        - Raises AuthorizationError if not server admin privileges.
        - Raises CreationError if a database with that name already exists.
        - Raises IOError if there is some other error.
        """
        self.server._PUT(self.name)
        return self

    def destroy(self):
        """Delete this database and all its contents.
        - Raises AuthorizationError if not server admin privileges.
        - Raises NotFoundError if no such database.
        - Raises IOError if there is some other error.
        """
        self.server._DELETE(self.name)

    def get_info(self):
        "Return a dictionary containing information about the database."
        return self.server._GET(self.name).json()

    def compact(self):
        "Compact the database on disk. May take some time."
        self.server._POST(self.name, '_compact',
                          headers={'Content-Type': JSON_MIME})

    def is_compact_running(self):
        "Is a compact operation running?"
        response = self.server._GET(self.name)
        return response.json()['compact_running']

    def get(self, id, rev=None, revs_info=False, default=None):
        """Return the document with the given id.
        - Returns the default if not found.
        - Raises AuthorizationError if not read privilege.
        - Raises IOError if there is some other error.
        """
        params = {}
        if rev is not None:
            params['rev'] = rev
        if revs_info:
            params['revs_info'] = json.dumps(True)
        response = self.server._GET(self.name, id,
                                    errors={404: None}, params=params)
        if response.status_code == 404:
            return default
        return response.json(object_pairs_hook=collections.OrderedDict)

    def save(self, doc):
        """Insert or update the document.

        If the document does not contain an item '_id', it is added
        having a UUID4 value. The '_rev' item is added or updated.

        - Raises NotFoundError if the database does not exist.
        - Raises AuthorizationError if not privileged to write.
        - Raises RevisionError if the '_rev' item does not match.
        - Raises IOError if something else went wrong.
        """
        if '_id' not in doc:
            doc['_id'] = uuid.uuid4().hex
        response = self.server._PUT(self.name, doc['_id'], json=doc)
        doc['_rev'] = response.json()['rev']

    def delete(self, doc):
        """Delete the document.
        - Raises NotFoundError if no such document or no '_id' item.
        - Raises RevisionError if no '_rev' item, or it does not match.
        - Raises ValueError if the request body or parameters are invalid.
        - Raises IOError if something else went wrong.
        """
        if '_rev' not in doc:
            raise RevisionError("missing '_rev' item in the document")
        if '_id' not in doc:
            raise NotFoundError("missing '_id' item in the document")
        response = self.server._DELETE(self.name, doc['_id'], 
                                       headers={'If-Match': doc['_rev']})

    def load_design(self, name, doc, rebuild=True):
        """Load the design document under the given name.

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
        """
        response = self.server._GET(self.name, '_design', name,
                                    errors={404: None})
        if response.status_code == 200:
            current_doc = response.json()
            doc['_id'] = current_doc['_id']
            doc['_rev'] = current_doc['_rev']
            if doc == current_doc: 
                return False
        response = self.server._PUT(self.name, '_design', name, json=doc)
        if rebuild:
            for view in doc.get('views', {}):
                self.view(name, view, limit=1)
        return True

    def view(self, designname, viewname, key=None, keys=None,
             startkey=None, endkey=None,
             skip=None, limit=None, sorted=True, descending=False,
             group=False, group_level=None, reduce=None,
             include_docs=False):
        """Return a ViewResult object, containing the following attributes:
        - `rows`: the list of Row objects.
        - `offset`: the offset used for this set of rows.
        - `total_rows`: the total number of rows selected.

        A Row object contains the following attributes:
        - `id`: the identifier of the document, if any.
        - `key`: the key for the index row.
        - `value`: the value for the index row.
        - `doc`: the document, if any.
        """
        params = {}
        if startkey is not None:
            params['startkey'] = json.dumps(startkey)
        if key is not None:
            params['key'] = json.dumps(key)
        if keys is not None:
            params['keys'] = json.dumps(keys)
        if endkey is not None:
            params['endkey'] = json.dumps(endkey)
        if skip is not None:
            params['skip'] = json.dumps(skip)
        if limit is not None:
            params['limit'] = json.dumps(limit)
        if not sorted:
            params['sorted'] = json.dumps(False)
        if descending:
            params['descending'] = json.dumps(True)
        if group:
            params['group'] = json.dumps(True)
        if group_level is not None:
            params['group_level'] = json.dumps(group_level)
        if reduce is not None:
            params['reduce'] = json.dumps(bool(reduce))
        if include_docs:
            params['include_docs'] = json.dumps(True)
        response = self.server._GET(self.name, '_design', designname, '_view',
                                    viewname, params=params)
        data = response.json()
        return ViewResult([Row(r.get('id'), r.get('key'), r.get('value'),
                               r.get('doc')) for r in data.get('rows', [])],
                          data.get('offset'),
                          data.get('total_rows'))

    def load_index(self, fields, id=None, name=None, selector=None):
        """Load a Mango index specification.

        - 'fields' is a list of fields to index.
        - 'id' is the design document name.
        - 'name' is the view name.
        - 'selector' is a partial filter selector.

        Returns a dictionary with items 'id' (design document name),
        'name' (index name) and 'result' ('created' or 'exists').

        - Raises BadRequestError if the index is malformed.
        - Raises AuthorizationError if not server admin privileges.
        - Raises ServerError if there is an internal server error.
        """
        data = {'index': {'fields': fields}}
        if id is not None:
            data['ddoc'] = id
        if name is not None:
            data['name'] = name
        if selector is not None:
            data['index']['partial_filter_selector'] = selector
        response = self.server._POST(self.name, '_index', json=data)
        return response.json()

    def find(self, selector, limit=None, skip=None, sort=None, fields=None,
             use_index=None, bookmark=None, update=None):
        """Select documents according to the selector.

        Returns a dictionary with items 'docs', 'warning', 'execution_stats'
        and 'bookmark'.

        - Raises BadRequestError if the selector is malformed.
        - Raises AuthorizationError if not privileged to read.
        - Raises ServerError if there is an internal server error.
        """
        data = {'selector': selector}
        if limit is not None:
            data['limit'] = limit
        if skip is not None:
            data['skip'] = skip
        if sort is not None:
            data['sort'] = sort
        if fields is not None:
            data['fields'] = fields
        if use_index is not None:
            data['use_index'] = use_index
        if bookmark is not None:
            data['bookmark'] = bookmark
        if update is not None:
            data['update'] = update
        response = self.server._POST(self.name, '_find', json=data)
        return response.json(object_pairs_hook=collections.OrderedDict)

    def put_attachment(self, doc, content, filename=None, content_type=None):
        """'content' is a string or a file-like object. Return the new
        revision of the document.

        If no filename, then an attempt is made to get it from content object.

        - Raises ValueError if no filename is available.
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

    def get_attachment(self, doc, filename):
        "Return a file-like object containing the content of the attachment."
        response = self.server._GET(self.name, doc['_id'], filename,
                                    headers={'If-Match': doc['_rev']})
        return io.BytesIO(response.content)

    def delete_attachment(self, doc, filename):
        "Delete the attachment. Return the new revision of the document."
        response = self.server._DELETE(self.name, doc['_id'], filename,
                                       headers={'If-Match': doc['_rev']})
        return response.json()['rev']

    def dump(self, filepath, progress_func=None):
        """Dump the entire database to the named tar file.

        If defined, the function `progress_func(ndocs, nfiles)` is called 
        every 100 documents.

        If the filepath ends with `.gz`, then the tar file is gzip compressed.
        The `_rev` item of each document is kept.

        A tuple (ndocs, nfiles) is returned.
        """
        nfiles = 0
        if filepath.endswith('.gz'):
            mode = 'w:gz'
        else:
            mode = 'w'
        with tarfile.open(filepath, mode=mode) as outfile:
            for ndocs, doc in enumerate(self):
                info = tarfile.TarInfo(doc['_id'])
                data = json.dumps(doc)
                info.size = len(data)
                outfile.addfile(info, io.BytesIO(data))
                # Attachments must follow their document.
                for attname in doc.get('_attachments', dict()):
                    info = tarfile.TarInfo("{0}_att/{1}".format(
                        doc['_id'], attname))
                    attfile = self.get_attachment(doc, attname)
                    if attfile is None:
                        attdata = ''
                    else:
                        attdata = attfile.read()
                        attfile.close()
                    info.size = len(data)
                    outfile.addfile(info, io.BytesIO(attdata))
                    nfiles += 1
                if ndocs % 100 == 0 and progress_func:
                    progress_func(ndocs, nfiles)
        return (ndocs, nfiles)

    def undump(self, filepath, progress_func=None):
        """Load the named tar file, which must have been produced by `dump`.

        If defined, the function `progress_func(ndocs, nfiles)` is called 
        every 100 documents.

        NOTE: The documents are just added to the database, ignoring any
        `_rev` items. This implies that they may not already exist.

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
                    doc = json.loads(itemdata, 
                                     object_pairs_hook=collections.OrderedDict)
                    doc.pop('_rev', None)
                    atts = doc.pop('_attachments', dict())
                    self.save(doc)
                    ndocs += 1
                    for attname, attinfo in atts.items():
                        key = "{0}_att/{1}".format(doc['_id'], attname)
                        atts[key] = dict(filename=attname,
                                         content_type=attinfo['content_type'])
                if ndocs % 100 == 0 and progress_func:
                    progress_func(ndocs, nfiles)
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
            data = response.json()
            rows = data['rows']
            if len(rows) == 0:
                raise StopIteration
            self.chunk = [r['doc'] for r in rows]
            self.chunk.reverse()
            self.skip = data['offset'] + len(self.chunk)
            return self.chunk.pop()


ViewResult = collections.namedtuple('ViewResult',
                                    ['rows', 'offset', 'total_rows'])
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
    "Wrong or missing '_rev' item in the document to save."

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


def get_parser():
    "Get the parser for the command line tool."
    p = argparse.ArgumentParser(description='CouchDB2 command line tool')
    p.add_argument('-v', '--verbose', action='store_true',
                   help='print more information')
    p.add_argument('-S', '--settings',
                   help='settings file in JSON format')
    p.add_argument('-s', '--server', help='server URL, including port number')
    p.add_argument('-d', '--database', help='database to operate on')
    p.add_argument('-u', '--username', help='CouchDB user account name')
    p.add_argument('-p', '--password', help='CouchDB user account password')
    p.add_argument('-P', '--interactive_password', action='store_true',
                   help='ask for the password by interactive input')
    p.add_argument('-o', '--output', metavar='FILEPATH',
                   help='write output to the given file (usually JSON format)')
    p.add_argument('--indent', type=int, metavar='INT',
                   help='indentation level for JSON format output file')
    p.add_argument('-f', '--force', action='store_true',
                   help='do not ask for interactive confirmation (delete, destroy)')

    g0 = p.add_argument_group('server operations')
    g0.add_argument('-V', '--version', action='store_true',
                    help='output CouchDB server version')
    g0.add_argument('--list', action='store_true',
                    help='output a list of the databases on the server')

    g1 = p.add_argument_group('database operations')
    g1.add_argument('--info', action='store_true',
                     help='output information about the database')
    x11 = g1.add_mutually_exclusive_group()
    x11.add_argument('--create', action='store_true',
                     help='create the database')
    x11.add_argument('--destroy', action='store_true',
                     help='delete the database and all its contents')
    x11.add_argument('--compact', action='store_true',
                     help='compact the database; may take some time')
    x12 = g1.add_mutually_exclusive_group()
    x12.add_argument('--dump', metavar='FILEPATH',
                     help='create a dump file of the database')
    x12.add_argument('--undump', metavar='FILEPATH',
                     help='load a dump file into the database')

    g2 = p.add_argument_group('document operations')
    x2 = g2.add_mutually_exclusive_group()
    x2.add_argument('--save', metavar='DOC_OR_FILEPATH',
                    help='save the document (from file or explicit)')
    x2.add_argument('--get', metavar="DOCID",
                    help='output the document with the given identifier')
    x2.add_argument('--delete', metavar="DOCID", # XXX
                    help='delete the document with the given identifier')

    g3 = p.add_argument_group('attachments to document')
    x3 = g3.add_mutually_exclusive_group()
    x3.add_argument('--attach', nargs=2, metavar=('DOCID', 'FILEPATH'),
                    help='attach the specified file to the given document')
    x3.add_argument('--detach', nargs=2, metavar=('DOCID', 'FILENAME'),
                    help='remove the attached file from the given document')
    x3.add_argument('--getfile', nargs=2, metavar=('DOCID', 'FILENAME'),
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


DEFAULT_SETTINGS = {
    'SERVER': 'http://localhost:5984',
    'DATABASE': None,
    'USERNAME': None,
    'PASSWORD': None
}

def get_settings(pargs, filepaths=['~/.couchdb2', 'settings.json']):
    """Get the settings lookup.
    1) Initialize with default settings.
    2) Update with values in JSON file '~/.couchdb2', if any.
    3) Update with values in JSON file 'settings.json' (current dir), if any.
    4) Update with values in the explicitly given settings file, if any.
    5) Modify by any command line arguments.
    """
    settings = DEFAULT_SETTINGS.copy()
    filepaths = filepaths[:]
    if pargs.settings:
        filepaths.append(pargs.settings)
    for filepath in filepaths:
        try:
            with open(os.path.expanduser(filepath), 'rb') as infile:
                settings.update(json.load(infile))
            verbosity(pargs, 'settings read from file', filepath)
        except IOError:
            verbosity(pargs, 'Warning: could not read settings file', filepath)
    for key in ['COUCHDB_SERVER', 'COUCHDB2_SERVER']:
        try:
            settings['SERVER'] = settings[key]
        except KeyError:
            pass
    for key in ['COUCHDB_DATABASE', 'COUCHDB2_DATABASE']:
        try:
            settings['DATABASE'] = settings[key]
        except KeyError:
            pass
    for key in ['COUCHDB_USER', 'COUCHDB2_USER', 
                'COUCHDB_USERNAME', 'COUCHDB2_USERNAME']:
        try:
            settings['USERNAME'] = settings[key]
        except KeyError:
            pass
    for key in ['COUCHDB_PASSWORD', 'COUCHDB2_PASSWORD']:
        try:
            settings['PASSWORD'] = settings[key]
        except KeyError:
            pass
    if pargs.server:
        settings['SERVER'] = pargs.server
    if pargs.database:
        settings['DATABASE'] = pargs.database
    if pargs.username:
        settings['USERNAME'] = pargs.username
    if pargs.password:
        settings['PASSWORD'] = pargs.password
    if pargs.verbose:
        s = collections.OrderedDict()
        for key in ['SERVER', 'DATABASE', 'USERNAME']:
            s[key] = settings[key]
        if settings['PASSWORD'] is None:
            s['PASSWORD'] = None
        else:
            s['PASSWORD'] = '****'
        verbosity(pargs, 'settings:', json.dumps(s))
    return settings

def get_database(server, settings):
    if not settings['DATABASE']:
        sys.exit('error: no database defined')
    return server[settings['DATABASE']]

def verbosity(pargs, *args):
    "If flag '--verbose' used, print the arguments."
    if not pargs.verbose: return
    print(*args)

def to_json(pargs, data):
    """If '--output' was used, write the data in JSON format to the file.
    The indentation level is set by '--indent'.
    If the filepath ends in '.gz'. then a gzipped file is produced.
    """
    if pargs.output:
        if pargs.output.endswith('.gz'):
            with gzip.open(pargs.output, 'wb') as outfile:
                json.dump(data, outfile, indent=pargs.indent)
        else:
            with open(pargs.output, 'wb') as outfile:
                json.dump(data, outfile, indent=pargs.indent)
        verbosity(pargs, 'wrote to file', pargs.output)
    return bool(pargs.output)

def print_dot(*args):
    print('.', sep='', end='')
    sys.stdout.flush()

def main(pargs, settings):
    "CouchDB2 command line tool."
    try:
        input = raw_input
    except NameError:
        pass
    if pargs.interactive_password:
        settings['PASSWORD'] = getpass.getpass('password > ')
    server = Server(href=settings['SERVER'],
                    username=settings['USERNAME'],
                    password=settings['PASSWORD'])
    if pargs.version:
        if not to_json(pargs, server.version):
            print(server.version)
    if pargs.list:
        dbs = list(server)
        if not to_json(pargs, [str(db) for db in dbs]):
            for db in dbs:
                print(db)

    if pargs.info:
        doc = get_database(server, settings).get_info()
        if not to_json(pargs, doc):
            print(json.dumps(db, indent=2))
    elif pargs.create:
        db = server.create(settings['DATABASE'])
        verbosity(pargs, 'created database', db)
    elif pargs.destroy:
        db = get_database(server, settings)
        if not pargs.force:
            answer = input("really destroy database '{}' [n] ? ".format(db))
            if answer and answer.lower()[0] in ('y', 't'):
                pargs.force = True
        if pargs.force:
            db.destroy()
            verbosity(pargs, 'destroyed database', settings['DATABASE'])
    elif pargs.compact:
        db = get_database(server, settings)
        db.compact()
        print("compacting '{}'.".format(db), sep='', end='')
        sys.stdout.flush()
        while db.is_compact_running():
            time.sleep(1)
            print_dot()
        print()

    if pargs.save:
        db = get_database(server, settings)
        try:                    # Attempt to interpret arg as explicit doc
            doc = json.loads(pargs.save,
                             object_pairs_hook=collections.OrderedDict)
        except (ValueError, TypeError): # Arg is filepath to doc
            try:
                with open(pargs.save, 'rb') as infile:
                    doc = json.load(infile,
                                    object_pairs_hook=collections.OrderedDict)
            except (IOError, ValueError, TypeError) as error:
                sys.exit("Error: {}".format(error))
        id = doc.get('_id')
        db.save(doc)
        if id:
            verbosity(pargs, 'saved doc', doc['_id'])
        else:
            print('saved doc', doc['_id'])
    elif pargs.get:
        doc = get_database(server, settings)[pargs.get]
        if not to_json(pargs, doc):
            print(json.dumps(doc, indent=2))
    elif pargs.delete:
        db = get_database(server, settings)
        doc = db[pargs.delete]
        db.delete(doc)
        verbosity(pargs, 'deleted doc', doc['_id'])

    if pargs.attach:
        db = get_database(server, settings)
        doc = db[pargs.attach[0]]
        with open(pargs.attach[1], 'rb') as infile:
            db.put_attachment(doc, infile, 
                              filename=os.path.basename(pargs.attach[1]))
        verbosity(pargs, 
                  "attached file '{1}' to doc '{0}'".format(*pargs.attach))
    elif pargs.detach:
        db = get_database(server, settings)
        doc = db[pargs.detach[0]]
        db.delete_attachment(doc, pargs.detach[1])
        verbosity(pargs,
                  "detached file '{1}' from doc '{0}'".format(*pargs.detach))
    elif pargs.getfile:
        db = get_database(server, settings)
        doc = db[pargs.getfile[0]]
        filepath = pargs.output or pargs.getfile[1]
        with open(filepath, 'wb') as outfile:
            outfile.write(db.get_attachment(doc, pargs.getfile[1]).read())
        verbosity(pargs,
                  "wrote file '{0}' from doc '{1}' attachment '{2}'".
                  format(filepath, *pargs.getfile))

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
        data = collections.OrderedDict()
        data['total_rows'] = result.total_rows
        data['offset'] = result.offset
        data['rows'] = rows = [r._asdict() for r in result.rows]
        if not to_json(pargs, data):
            print(json.dumps(data, indent=2))
    if pargs.dump:
        db = get_database(server, settings)
        print('dumping', sep='', end='')
        sys.stdout.flush()
        ndocs, nfiles = db.dump(pargs.dump, progress_func=print_dot)
        print()
        print('dumped', ndocs, 'documents,', nfiles, 'files')
    elif pargs.undump:
        db = get_database(server, settings)
        if len(db) != 0:
            sys.exit("database '{}' is not empty".format(db))
        print('undumping', sep='', end='')
        sys.stdout.flush()
        ndocs, nfiles = db.undump(pargs.undump, progress_func=print_dot)
        print()
        print('undumped', ndocs, 'documents,', nfiles, 'files')


if __name__ == '__main__':
    try:
        parser = get_parser()
        pargs = parser.parse_args()
        settings = get_settings(pargs)
        main(pargs, settings)
    except CouchDB2Exception as error:
        sys.exit("Error: {}".format(error))
