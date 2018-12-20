"""Slim Python interface module to CouchDB v2.x.

Relies on requests, http://docs.python-requests.org/en/master/
"""

from __future__ import print_function

__version__ = '0.9.1'

import collections
from collections import OrderedDict as OD
import json
import mimetypes
try:                            # Python 2
    from StringIO import StringIO as ContentFile
    ContentFile.read = ContentFile.getvalue
except ImportError:             # Python 3
    from io import BytesIO as ContentFile
    ContentFile.read = ContentFile.getvalue
import uuid

import requests

JSON_MIME = 'application/json'
BIN_MIME  = 'application/octet-stream'


class Server(object):
    "Connection to the CouchDB server."

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
            raise NotFoundError('database does not exist')

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
        return response.json(object_pairs_hook=OD)

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
        "Return the selected rows from the named design view."
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
        return response.json(object_pairs_hook=OD)

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
        return ContentFile(response.content)


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
    404: NotFoundError('no such entity'),
    409: RevisionError("missing or incorrect '_rev' item"),
    412: CreationError('name already in use'),
    415: ContentTypeError("bad 'Content-Type' value"),
    500: ServerError('internal server error')}


if __name__ == '__main__':
    import time
    server = Server()
    try:
        db = server.get('mytest')
    except NotFoundError:
        db = server.create('mytest')
    doc = {'type': 'adoc', 'name': 'blah'}
    db.save(doc)
    rev = doc['_rev']
    doc['other'] = 'stuff'
    db.save(doc)
    id1 = {'id': doc['_id'], 'rev': rev}
    id1x = {'id': doc['_id'], 'rev': doc['_rev']}
    doc = {'type': 'adoc', 'name': 'blopp'}
    db.save(doc)
    id2 = doc['_id']
    doc['fruit'] = 'banana'
    db.save(doc)
    id3 = doc['_id']
    db.compact()
    while db.is_compact_running():
        print('sleeping...')
        time.sleep(0.5)
    print(json.dumps(db.get(id3, revs_info=True), indent=2))
    db.destroy()
