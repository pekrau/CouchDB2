# -*- coding: utf-8 -*-
"""Slim Python interface module to CouchDB (version 2).
Relies on requests, http://docs.python-requests.org/en/master/
"""

from __future__ import print_function, unicode_literals

import mimetypes
try:
    from StringIO import StringIO as ContentFile
    ContentFile.read = ContentFile.getvalue
except ImportError:
    from io import BytesIO as ContentFile
    ContentFile.read = ContentFile.getvalue
import uuid

import requests

__version__ = '0.3.0'

JSON_MIME = 'application/json'
BIN_MIME  = 'application/octet-stream'

class CouchDB2Exception(Exception):
    "Base CouchDB2 exception class."

class NotFoundError(CouchDB2Exception, KeyError):
    "No such entity exists."

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
        response = self._get()
        if response.status_code != 200:
            raise IOError('could not connect to the server')
        self.version = response.json()['version']

    def __str__(self):
        return "CouchDB {s.version} {s.href}".format(s=self)

    def __len__(self):
        "Number of user-defined databases."
        data = self._get('_all_dbs').json()
        return len([n for n in data if not n.startswith('_')])

    def __iter__(self):
        "Iterate over all user-defined databases on the server."
        data = self._get('_all_dbs').json()
        return iter([Database(self, n, check=False) 
                     for n in data if not n.startswith('_')])

    def __getitem__(self, name):
        """Get the named database.
        Raises NotFoundError if no such database.
        """
        return Database(self, name)

    def __contains__(self, name):
        "Does the named database exist?"
        response = self._head(name)
        return response.status_code == 200

    def _get(self, *segments, **kwargs):
        "HTTP GET request to the CouchDB server."
        return self._session.get(self._href(segments),
                                 **self._kwargs(kwargs, 'headers'))

    def _head(self, *segments):
        "HTTP HEAD request to the CouchDB server."
        return self._session.head(self._href(segments))

    def _put(self, *segments, **kwargs):
        "HTTP PUT request to the CouchDB server."
        return self._session.put(self._href(segments),
                                 **self._kwargs(kwargs, 
                                                'json', 'data', 'headers'))

    def _delete(self, *segments, **kwargs):
        """HTTP DELETE request to the CouchDB server.
        Pass parameters in the keyword argument 'params'.
        """
        return self._session.delete(self._href(segments),
                                    **self._kwargs(kwargs, 'headers'))

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

    def create(self, name):
        "Create the named database."
        database = Database(self, name, check=False)
        database.create()
        return database


class Database(object):
    "Interface to a named CouchDB database."

    def __init__(self, server, name, check=True):
        self.server = server
        self.name = name
        if check:
            self.check()

    def __str__(self):
        return "CouchDB database {s.name}".format(s=self)

    def __len__(self):
        "Return the number of documents in the database."
        response = self.server._get(self.name)
        if response.status_code != 200:
            raise ValueError("{r.status_code} {r.reason}".format(r=response))
        return response.json()['doc_count']

    def __contains__(self, id):
        "Does a document with the given id exist in the database?"
        response = self.server._head(self.name, id)
        if response.status_code == 401:
            raise AuthorizationError('read privilege required')
        return response.status_code in (200, 304)

    def __getitem__(self, id):
        """Return the document with the given id.
        Raise NotFoundError if no such document.
        """
        response = self.server._get(self.name, id)
        if response.status_code == 200:
            return response.json()
        elif response.status_code == 401:
            raise AuthorizationError('read privilege required')
        elif response.status_code == 404:
            raise NotFoundError('no such document')
        else:
            raise IOError("{r.status_code} {r.reason}".format(r=response))

    def exists(self):
        "Does this database exist?"
        response = self.server._head(self.name)
        return response.status_code == 200

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
        response = self.server._put(self.name)
        if response.status_code in (201, 202):
            return self
        elif response.status_code == 400:
            raise ValueError("invalid database name {}".format(self.name))
        elif response.status_code == 401:
            raise AuthorizationError('server admin privileges required')
        elif response.status_code == 412:
            raise CreationError('database name already in use')
        else:
            raise IOError("{r.status_code} {r.reason}".format(r=response))

    def destroy(self):
        """Delete this database and all its contents.
        Raises AuthorizationError if not server admin privileges.
        Raises NotFoundError if no such database.
        Raises IOError if there is some other error.
        """
        response = self.server._delete(self.name)
        if response.status_code in (200, 202):
            pass
        elif response.status_code == 401:
            raise AuthorizationError('server admin privileges required')
        elif response.status_code in (400, 404):
            raise NotFoundError('no such database')
        else:
            raise IOError("{r.status_code} {r.reason}".format(r=response))

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
        If the document does not contain an item '_id', it is added with
        a UUID4 value. The '_rev' item is added or updated.
        Raises NotFoundError if the database does not exist.
        Raises AuthorizationError if not privileged to write.
        Raises RevisionError if the '_rev' item does not match.
        Raises IOError if something else went wrong.
        """
        if '_id' not in doc:
            doc['_id'] = uuid.uuid4().hex
        response = self.server._put(self.name, doc['_id'], json=doc)
        if response.status_code in (201, 202):
            data = response.json()
            if not data.get('ok'):
                raise IOError('response not OK')
            doc['_rev'] = data['rev']
        elif response.status_code == 400:
            raise ValueError('invalid request body or parameters')
        elif response.status_code == 401:
            raise AuthorizationError('write privilege required')
        elif response.status_code == 404:
            raise NotFoundError('no such database')
        elif response.status_code == 409:
            raise RevisionError("missing or incorrect '_rev' item")
        else:
            raise IOError("{r.status_code} {r.reason}".format(r=response))

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
        response = self.server._delete(self.name, doc['_id'], 
                                       headers={'If-Match': doc['_rev']})
        if response.status_code in (200, 202):
            data = response.json()
            if not data.get('ok'):
                raise IOError('response not OK')
        elif response.status_code == 400:
            raise ValueError('invalid request body or parameters')
        elif response.status_code == 401:
            raise AuthorizationError('write privilege required')
        elif response.status_code == 404:
            raise NotFoundError('no such document')
        elif response.status_code == 409:
            raise RevisionError("missing or incorrect '_rev' item")
        else:
            raise IOError("{r.status_code} {r.reason}".format(r=response))

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
        response = self.server._put(self.name, doc['_id'], filename,
                                    data=content,
                                    headers={'Content-Type': content_type,
                                             'If-Match': doc['_rev']})
        if response.status_code in (201, 202):
            pass
        elif response.status_code == 400:
            raise ValueError('invalid request body or parameters')
        elif response.status_code == 401:
            raise AuthorizationError('write privilege required')
        elif response.status_code == 404:
            raise NotFoundError('no such document')
        elif response.status_code == 409:
            raise RevisionError("missing or incorrect '_rev' item")
        else:
            raise IOError("{r.status_code} {r.reason}".format(r=response))

    def get_attachment(self, doc, filename):
        "Return a file-like object containing the content of the attachment."
        response = self.server._get(self.name, doc['_id'], filename,
                                    headers={'If-Match': doc['_rev']})
        if response.status_code == 200:
            return ContentFile(response.content)
        elif response.status_code == 401:
            raise AuthorizationError('read privilege required')
        elif response.status_code == 404:
            raise NotFoundError('no such document or attachment')
        else:
            raise IOError("{r.status_code} {r.reason}".format(r=response))


if __name__ == '__main__':
    db = Server().create('mytest')
    doc = {'name': 'myfile', 'contents': 'a Python file'}
    db.save(doc)
    with open(__file__, 'rb') as infile:
        db.put_attachment(doc, infile)
    attfile = db.get_attachment(doc, __file__)
    attcontent = attfile.read()
    print(len(attcontent))
    with open(__file__, 'rb') as infile:
        assert attcontent == infile.read()
