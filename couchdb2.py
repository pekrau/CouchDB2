# -*- coding: utf-8 -*-
"""Slim Python interface module to CouchDB (version 2).
Relies on requests, http://docs.python-requests.org/en/master/
"""

from __future__ import print_function, unicode_literals

import collections
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

__version__ = '0.4.0'

JSON_MIME = 'application/json'
BIN_MIME  = 'application/octet-stream'

ViewResult = collections.namedtuple('ViewResult',
                                    ['rows', 'offset', 'total_rows'])
Row = collections.namedtuple('Row', ['id', 'key', 'value', 'doc'])

class CouchDB2Exception(Exception):
    "Base CouchDB2 exception class."

class NotFoundError(KeyError, CouchDB2Exception):
    "No such entity exists."

class InvalidRequest(CouchDB2Exception):
    "Invalid request; bad name, body or headers."

class CreationError(CouchDB2Exception):
    "Could not create the entity; it exists already."

class RevisionError(CouchDB2Exception):
    "Wrong or missing '_rev' item in the document to save."

class AuthorizationError(CouchDB2Exception):
    "Current user not authorized to perform the operation."


class Server(object):
    "Connection to the CouchDB server."

    def __init__(self, href='http://localhost:5984/',
                 username=None, password=None):
        "Raises IOError if server connection failed."
        self.href = href.rstrip('/') + '/'
        self.username = username
        self.password = password
        self._session = requests.Session()
        if self.username and self.password:
            self._session.auth = (self.username, self.password)
        self._session.headers.update({'Accept': JSON_MIME})
        self.version = self._GET().json()['version']

    def __str__(self):
        return "CouchDB {s.version} {s.href}".format(s=self)

    def __len__(self):
        "Number of user-defined databases."
        data = self._GET('_all_dbs').json()
        return len([n for n in data if not n.startswith('_')])

    def __iter__(self):
        "Iterate over all user-defined databases on the server."
        data = self._GET('_all_dbs').json()
        return iter([Database(self, n, check=False) 
                     for n in data if not n.startswith('_')])

    def __getitem__(self, name):
        """Get the named database.
        Raises NotFoundError if no such database.
        """
        return Database(self, name, check=True)

    def __contains__(self, name):
        "Does the named database exist?"
        response = self._HEAD(name)
        return response.status_code == 200

    def get(self, name, check=True):
        """Get the named database.
        Raises NotFoundError if 'check' is True and the database does not exist.
        """
        return Database(self, name, check=check)

    def create(self, name):
        """Create the named database.
        Raises ValueError if the name is invalid.
        Raises AuthorizationError if not server admin privileges.
        Raises CreationError if a database with that name already exists.
        Raises IOError if there is some other error.
        """
        return Database(self, name, check=False).create()

    def _HEAD(self, *segments):
        "HTTP HEAD request to the CouchDB server."
        return self._session.head(self._href(segments))

    def _GET(self, *segments, **kwargs):
        "HTTP GET request to the CouchDB server."
        kw = self._kwargs(kwargs, 'headers', 'params')
        return self._session.get(self._href(segments), **kw)

    def _PUT(self, *segments, **kwargs):
        "HTTP PUT request to the CouchDB server."
        kw = self._kwargs(kwargs, 'json', 'data', 'headers')
        return self._session.put(self._href(segments), **kw)

    def _DELETE(self, *segments, **kwargs):
        """HTTP DELETE request to the CouchDB server.
        Pass parameters in the keyword argument 'params'.
        """
        kw = self._kwargs(kwargs, 'headers')
        return self._session.delete(self._href(segments), **kw)

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

    _default_errors = {
        200: None,
        201: None,
        202: None,
        304: None,
        400: InvalidRequest('bad name, request body or parameters'),
        401: AuthorizationError('insufficient privilege'),
        404: NotFoundError('no such entity'),
        409: RevisionError("missing or incorrect '_rev' item"),
        412: CreationError('name already in use')}

    def _check(self, response, errors={}):
        "Raise an exception if the response status code indicates an error."
        try:
            error = errors[response.status_code]
        except KeyError:
            try:
                error = self._default_errors[response.status_code]
            except KeyError:
                raise IOError("{r.status_code} {r.reason}".format(r=response))
        if error is not None:
            print(json.dumps(response.json()))
            raise error


class Database(object):
    "Interface to a named CouchDB database."

    def __init__(self, server, name, check=True):
        self.server = server
        self.name = name
        if check:
            self.check()

    def __str__(self):
        return self.name

    def __len__(self):
        "Return the number of documents in the database."
        return self.server._GET(self.name).json()['doc_count']

    def __contains__(self, id):
        """Does a document with the given id exist in the database?
        Raises AuthorizationError if not privileged to read.
        Raises IOError if something else went wrong.
        """
        response = self.server._HEAD(self.name, id)
        self.server._check(response, errors={404: None})
        return response.status_code in (200, 304)

    def __getitem__(self, id):
        """Return the document with the given id.
        Raises AuthorizationError if not privileged to read.
        Raises NotFoundError if no such document or database.
        Raises IOError if something else went wrong.
        """
        response = self.server._GET(self.name, id)
        self.server._check(response)
        return response.json()

    def exists(self):
        "Does this database exist?"
        return self.server._HEAD(self.name).status_code == 200

    def check(self):
        "Raises NotFoundError if this database does not exist."
        if not self.exists():
            raise NotFoundError('database does not exist')

    def create(self):
        """Create this database.
        Raises ValueError if the name is invalid.
        Raises AuthorizationError if not server admin privileges.
        Raises CreationError if a database with that name already exists.
        Raises IOError if there is some other error.
        """
        response = self.server._PUT(self.name)
        self.server._check(response)
        return self

    def destroy(self):
        """Delete this database and all its contents.
        Raises AuthorizationError if not server admin privileges.
        Raises NotFoundError if no such database.
        Raises IOError if there is some other error.
        """
        response = self.server._DELETE(self.name)
        self.server._check(response)

    def get(self, id, default=None):
        """Return the document with the given id.
        Returns the default if not found.
        Raises AuthorizationError if not read privilege.
        Raises IOError if there is some other error.
        """
        try:
            return self[id]
        except NotFoundError:
            return default

    def save(self, doc):
        """Insert or update the document.
        If the document does not contain an item '_id', it is added
        having a UUID4 value. The '_rev' item is added or updated.
        Raises NotFoundError if the database does not exist.
        Raises AuthorizationError if not privileged to write.
        Raises RevisionError if the '_rev' item does not match.
        Raises IOError if something else went wrong.
        """
        if '_id' not in doc:
            doc['_id'] = uuid.uuid4().hex
        response = self.server._PUT(self.name, doc['_id'], json=doc)
        self.server._check(response)
        doc['_rev'] = response.json()['rev']

    def delete(self, doc):
        """Delete the document.
        Raises NotFoundError if no such document.
        Raises RevisionError if the '_rev' item does not match.
        Raises ValueError if the request body or parameters are invalid.
        Raises IOError if something else went wrong.
        """
        if '_rev' not in doc:
            raise RevisionError("missing '_rev' item in the document")
        if '_id' not in doc:
            raise NotFoundError("missing '_id' item in the document")
        response = self.server._DELETE(self.name, doc['_id'], 
                                       headers={'If-Match': doc['_rev']})
        self.server._check(response)

    def load_design(self, name, doc):
        """Load the design document with the given name.
        If the existing design document is identical, no action and
        False is returned, else True is returned.
        See http://docs.couchdb.org/en/latest/api/ddoc/common.html for info
        on the structure of the ddoc.
        Raises AuthorizationError if not privileged to read.
        Raise NotFoundError if no such database.
        Raises IOError if something else went wrong.
        """
        response = self.server._GET(self.name, '_design', name)
        self.server._check(response, {404: None})
        if response.status_code == 200:
            current_doc = response.json()
            doc['_id'] = current_doc['_id']
            doc['_rev'] = current_doc['_rev']
            if doc == current_doc: 
                return False
        response = self.server._PUT(self.name, '_design', name, json=doc)
        self.server._check(response)
        return True

    def view(self, designname, viewname, startkey=None, endkey=None,
             skip=None, limit=None, descending=False,
             group=False, group_level=None, reduce=None,
             include_docs=False):
        "Return rows from the named design view."
        params = {}
        if startkey is not None:
            params['startkey'] = json.dumps(startkey)
        if endkey is not None:
            params['endkey'] = json.dumps(endkey)
        if skip is not None:
            params['skip'] = str(skip)
        if limit is not None:
            params['limit'] = str(limit)
        if descending:
            params['descending'] = 'true'
        if group:
            params['group'] = 'true'
        if group_level is not None:
            params['group_level'] = str(group_level)
        if reduce is not None:
            params['reduce'] = str.dumps(bool(reduce))
        if include_docs:
            params['include_docs'] = 'true'
        response = self.server._GET(self.name, '_design', designname, '_view',
                                    viewname, params=params)
        self.server._check(response)
        data = response.json()
        print(json.dumps(data, indent=2))
        return ViewResult([Row(r.get('id'), r.get('key'), r.get('value'),
                               r.get('doc')) for r in data.get('rows', [])],
                          data.get('offset'),
                          data.get('total_rows'))

    def put_attachment(self, doc, content, filename=None, content_type=None):
        """'content' is a string or a file-like object.
        If no filename, then an attempt is made to get it from content object.
        Raises ValueError if no filename is available.
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
        self.server._check(response)

    def get_attachment(self, doc, filename):
        "Return a file-like object containing the content of the attachment."
        response = self.server._GET(self.name, doc['_id'], filename,
                                    headers={'If-Match': doc['_rev']})
        self.server._check(response)
        return ContentFile(response.content)


if __name__ == '__main__':
    server = Server()
    try:
        db = server.get('mytest')
    except NotFoundError:
        db = server.create('mytest')
    print('stuff' in db)
    try:
        db['stuff']
    except NotFoundError:
        print('not found')
    doc = {'id': 'stuff', 'name': 'blah'}
    db.save(doc)
    print('stuff' in db)
    print(db.load_design('all', 
                         {'views':
                          {'name':
                           {'map': "function (doc) {emit(doc.name, null);}"}}}))
    result = db.view('all', 'name', include_docs=True)
    print(json.dumps(result, indent=2))
    db.destroy()
