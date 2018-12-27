"""Slim Python interface module to CouchDB v2.x.

Relies on requests: http://docs.python-requests.org/en/master/
"""

from __future__ import print_function

__version__ = '1.1.0'

import argparse
import collections
import getpass
import io
import json
import mimetypes
import os.path
import sys
import tarfile
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
            raise error


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

        - Raises AuthorizationError if not privileged to read.
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
        """Return the selected rows from the named design view.

        A #ViewResult object is returned, containing the following attributes:
        - `rows`: the list of #Row objects.
        - `offset`: the offset used for this set of rows.
        - `total_rows`: the total number of rows selected.

        A #Row object contains the following attributes:
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
        """'content' is a string or a file-like object.

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

    def get_attachment(self, doc, filename):
        "Return a file-like object containing the content of the attachment."
        response = self.server._GET(self.name, doc['_id'], filename,
                                    headers={'If-Match': doc['_rev']})
        return io.BytesIO(response.content)

    def dump(self, filepath):
        """Dump the entire database to the named tar file.
        If the filepath ends with '.gz', the tar file is gzip compressed.

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
                data = json.dumps(doc)
                info.size = len(data)
                outfile.addfile(info, io.BytesIO(data))
                ndocs += 1
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
        return (ndocs, nfiles)

    def undump(self, filepath):
        """Load the named tar file, which must have been produced by `dump`.

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
    400: BadRequestError('bad name, request body or parameters'),
    401: AuthorizationError('insufficient privilege'),
    403: AuthorizationError('insufficient privilege'),
    404: NotFoundError('no such entity'),
    409: RevisionError("missing or incorrect '_rev' item"),
    412: CreationError('name already in use'),
    415: ContentTypeError("bad 'Content-Type' value"),
    500: ServerError('internal server error')}


def get_parser():
    "Get the parser for the command line tool."
    p = argparse.ArgumentParser(description='CouchDB2 command line tool')
    p.add_argument('-v', '--verbose', action='store_true',
                   help='more verbose information')
    p.add_argument('-S', '--settings', default='~/.couchdb2',
                   help='settings file in JSON format (default ~/.couchdb2)')
    p.add_argument('-s', '--server', help='server URL, including port number')
    p.add_argument('-d', '--database', help='database to use')
    p.add_argument('-u', '--username', help='user name')
    p.add_argument('-p', '--password', help='password')
    p.add_argument('-P', '--interactive_password', action='store_true',
                   help='ask for the password by interactive input')
    p.add_argument('-V', '--version', action='store_true',
                   help='output CouchDB server version')
    p.add_argument('-L', '--list', action='store_true',
                   help='list the databases on the server')
    p.add_argument('-i', '--information', action='store_true',
                   help='output information about the database')
    p.add_argument('-X', '--delete', action='store_true',
                   help='delete the database')
    p.add_argument('-f', '--force', action='store_true',
                   help='do not ask for interactive confirmation')
    p.add_argument('-C', '--create', action='store_true',
                   help='create the database')
    p.add_argument('-c', '--dump', metavar='FILENAME',
                   help='create a dump file for the database')
    p.add_argument('-x', '--undump', metavar='FILENAME',
                   help='load a dump file into the database')
    p.add_argument('-a', '--save', metavar="FILENAME_OR_DOC",
                   help='save the document (file or explicit) in the database')
    p.add_argument('-g', '--get', metavar="ID",
                   help='get the document with the given identifier')
    return p


DEFAULT_SETTINGS = {
    'SERVER': 'http://localhost:5984',
    'DATABASE': None,
    'USERNAME': None,
    'PASSWORD': None
}

def get_settings(pargs):
    """Get the settings lookup.
    1) Initialize with default settings.
    2) Update with values in the settings file (if any).
    3) Modify by any command line arguments.
    """
    settings = DEFAULT_SETTINGS.copy()
    if pargs.settings:
        try:
            with open(os.path.expanduser(pargs.settings), 'r') as infile:
                settings.update(json.load(infile))
            if pargs.verbose:
                print('settings from file', pargs.settings)
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
        except IOError:
            if pargs.verbose:
                print('Warning: could not read settings file', pargs.settings)
    if pargs.server:
        settings['SERVER'] = pargs.server
    if pargs.database:
        settings['DATABASE'] = pargs.database
    if pargs.username:
        settings['USERNAME'] = pargs.username
    if pargs.password:
        settings['PASSWORD'] = pargs.password
    return settings

def get_database(server, settings):
    if not settings['DATABASE']:
        sys.exit('error: no database defined')
    return server[settings['DATABASE']]

def main():
    "CouchDB2 command line tool."
    try:
        input = raw_input
    except NameError:
        pass
    parser = get_parser()
    pargs = parser.parse_args()
    settings = get_settings(pargs)
    if pargs.interactive_password:
        settings['PASSWORD'] = getpass.getpass('password > ')
    server = Server(href=settings['SERVER'],
                    username=settings['USERNAME'],
                    password=settings['PASSWORD'])
    if pargs.version:
        print(server.version)
    if pargs.list:
        for db in server:
            print(db)
    if pargs.delete:
        db = get_database(server, settings)
        if not pargs.force:
            answer = input("really delete database '{}'? [n] > ".format(db))
            if answer and answer.lower()[0] in ('y', 't'):
                pargs.force = True
        if pargs.force:
            db.destroy()
            if pargs.verbose:
                print('deleted database', settings['DATABASE'])
    if pargs.create:
        db = server.create(settings['DATABASE'])
        if pargs.verbose:
            print('created database', db)
    if pargs.save:
        db = get_database(server, settings)
        try:
            doc = json.loads(pargs.save,
                             object_pairs_hook=collections.OrderedDict)
        except (ValueError, TypeError):
            try:
                with open(pargs.save, 'rb') as infile:
                    doc = json.load(infile,
                                    object_pairs_hook=collections.OrderedDict)
            except (IOError, ValueError, TypeError) as error:
                sys.exit("error: {}".format(error))
        db.save(doc)
        if pargs.verbose:
            print('saved doc', doc['_id'])
    if pargs.get:
        db = get_database(server, settings)
        print(json.dumps(db[pargs.get], indent=2))
    if pargs.dump:
        db = get_database(server, settings)
        ndocs, nfiles = db.dump(pargs.dump)
        print(ndocs, 'documents,', nfiles, 'files dumped')
    if pargs.undump:
        db = get_database(server, settings)
        if len(db) != 0:
            parser.error("database '{}' is not empty".format(db))
        ndocs, nfiles = db.undump(pargs.undump)
        print(ndocs, 'documents,', nfiles, 'files undumped')
    if pargs.information:
        db = get_database(server, settings)
        print(json.dumps(db.get_info(), indent=2))


if __name__ == '__main__':
    try:
        main()
    except CouchDB2Exception as error:
        sys.exit("error: {}".format(error))
