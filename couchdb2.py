# -*- coding: utf-8 -*-
"""Python module to interface with CouchDB (version 2).
Relies on requests, http://docs.python-requests.org/en/master/
"""

from __future__ import print_function, unicode_literals

import collections
import uuid

import requests

__version__ = '0.2.0'

JSON_MIME = 'application/json'

class CouchDB2Exception(Exception):
    "Base CouchDB2 exception class."

class ExistenceError(CouchDB2Exception, KeyError):
    "No such entity exists."

class CreationError(CouchDB2Exception):
    "Could not create the entity; it exists already."

class RevisionError(CouchDB2Exception):
    "Wrong or missing '_rev' item in the document to save."

class AuthorizationError(CouchDB2Exception):
    "Current user not authorized to perform the operation."

class OtherError(CouchDB2Exception):
    "Some other error."


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

    def __repr__(self):
        return "couchdb2.Server({s.href})".format(s=self)

    def __len__(self):
        "Number of user-defined databases."
        data = self._get('_all_dbs').json()
        return len([n for n in data if not n.startswith('_')])

    def __iter__(self):
        "Iterate over all user-defined databases on the server."
        data = self._get('_all_dbs').json()
        return iter([self[n] for n in data if not n.startswith('_')])

    def __getitem__(self, name):
        """Get the named database.
        Raises ExistenceError if no such database.
        """
        return Database(self, name)

    def __contains__(self, name):
        "Does the named database exist?"
        response = self._head(name)
        return response.status_code == 200

    def _get(self, *segments):
        "HTTP GET request to the CouchDB server."
        return self._session.get(self._href(segments))
        
    def _head(self, *segments):
        "HTTP HEAD request to the CouchDB server."
        return self._session.head(self._href(segments))

    def _put(self, *segments, **kwargs):
        "HTTP PUT request to the CouchDB server."
        kw = {}
        try:
            kw['json'] = kwargs['json']
        except KeyError:
            pass
        return self._session.put(self._href(segments), **kw)

    def _post(self, *segments, **kwargs):
        """HTTP POST request to the CouchDB server.
        Pass the data in the keyword argument 'json'.
        """
        kw = {}
        try:
            kw['json'] = kwargs['json']
        except KeyError:
            pass
        return self._session.post(self._href(segments), **kw)

    def _delete(self, *segments, **kwargs):
        """HTTP DELETE request to the CouchDB server.
        Pass parameters in the keyword argument 'params'.
        """
        kw = {}
        try:
            kw['params'] = kwargs['params']
        except KeyError:
            pass
        return self._session.delete(self._href(segments), **kw)

    def _href(self, segments):
        return self.href + '/'.join(segments)

    def create(self, name):
        """Create and return the named database.
        Raises ValueError if the name is invalid.
        Raises AuthorizationError if not server admin privileges.
        Raises CreationError if a database with that name already exists.
        Raises OtherError if there is some other error.
        """
        response = self._put(name)
        if response.status_code in (201, 202):
            return self[name]
        elif response.status_code == 400:
            raise ValueError("invalid database name {}".format(name))
        elif response.status_code == 401:
            raise AuthorizationError('server admin privileges required')
        elif response.status_code == 412:
            raise CreationError('database name conflict')
        else:
            raise OtherError("{r.status_code} {r.reason}".format(r=response))


class Database(object):
    "Interface to a CouchDB database."

    def __init__(self, server, name):
        """Raises ExistenceError if this database does not exist.
        Raises OtherError if there is some other error.
        """
        self.server = server
        self.name = name
        response = self.server._get(self.name)
        if response.status_code == 200:
            return
        elif response.status_code == 404:
            raise ExistenceError('no such database')
        else:
            raise OtherError("{r.status_code} {r.reason}".format(r=response))

    def __str__(self):
        return "CouchDB database {s.name}".format(s=self)

    def __repr__(self):
        return "couchdb2.Database({s.server!r}, {s.name})".format(s=self)

    def __len__(self):
        "Return the number of documents in the database."
        response = self.server._get(self.name)
        if response.status_code != 200:
            raise ValueError("{r.status_code} {r.reason}".format(r=response))
        return response.json()['doc_count']

    def __contains__(self, docid):
        "Does a document with the given docid exist in the database?"
        response = self.server._head(self.name, docid)
        if response.status_code == 401:
            raise AuthorizationError('read privilege required')
        return response.status_code in (200, 304)

    def __getitem__(self, docid):
        """Return the document with the given docid.
        Raise ExistenceError if no such document.
        """
        return self.get(docid)

    def __setitem__(self, docid, doc):
        "Save the document with the given docid."
        doc['_id'] = docid
        self.save(doc)

    def __delitem__(self, docid):
        self.delete(self[docid])

    def get(self, docid):
        """Return the document with the given docid.
        Raises ExistenceError if no such document.
        Raises OtherError if there is some other error.
        """
        response = self.server._get(self.name, docid)
        if response.status_code == 200:
            return response.json()
        elif response.status_code == 401:
            raise AuthorizationError('read privilege required')
        elif response.status_code == 404:
            raise ExistenceError('no such document')
        else:
            raise OtherError("{r.status_code} {r.reason}".format(r=response))

    def save(self, doc):
        """Insert or update the document. 
        If the document has an item '_rev', then update the document,
        else insert it. If the document does not contain an item '_id',
        add it with a UUID4 value. The '_rev' item is added or updated.
        Raises RevisionError if the '_rev' item does not match.
        """
        if '_rev' in doc:
            self.update(doc)
        else:
            self.insert(doc)

    def insert(self, doc):
        """Insert the document. Returns (docid, rev).
        If the document does not contain an item '_id', adds it with 
        a UUID4 value. The '_rev' item is added.
        Raises ExistenceError if the database does not exist.
        Raises AuthorizationError if not privileged to write.
        Raises CreationError if the document already exists.
        Raises OtherError if something else went wrong.
        """
        if '_rev' in doc:
            raise RevisionError("insert document may not contain a '_rev' item")
        if '_id' not in doc:
            doc['_id'] = uuid.uuid4().hex
        response = self.server._post(self.name, json=doc)
        if response.status_code in (201, 202):
            data = response.json()
            if not data.get('ok'):
                raise OtherError('response not OK')
            doc['_rev'] = data['rev']
            return (data['id'], data['rev'])
        elif response.status_code in (400, 404):
            raise ExistenceError(self.name)
        elif response.status_code == 401:
            raise AuthorizationError('write privilege required')
        elif response.status_code == 409:
            raise CreationError('document id conflict')
        else:
            raise OtherError("{r.status_code} {r.reason}".format(r=response))

    def update(self, doc):
        """Update the document.
        If the document does not contain an item '_id', adds it with
        a UUID4 value. The '_rev' item is updated.
        Raises AuthorizationError if not privileged to write.
        Raises RevisionError if the '_rev' item does not match.
        Raises OtherError if something else went wrong.
        """
        if '_rev' not in doc:
            raise RevisionError("missing '_rev' item in the document")
        if '_id' not in doc:
            doc['_id'] = uuid.uuid4().hex
        response = self.server._put(self.name, doc['_id'], json=doc)
        if response.status_code in (201, 202):
            data = response.json()
            if not data.get('ok'):
                raise OtherError('response not OK')
        elif response.status_code == 400:
            raise OtherError('invalid request body or parameters')
        elif response.status_code == 401:
            raise AuthorizationError('write privilege required')
        elif response.status_code == 404:
            raise ExistenceError('no such document')
        elif response.status_code == 409:
            raise RevisionError("document id conflict or incorrect '_rev' item")
        else:
            raise OtherError("{r.status_code} {r.reason}".format(r=response))
     
    def delete(self, doc):
        """Delete the document.
        Raises ExistenceError if no such document.
        Raises RevisionError if the '_rev' item does not match.
        """
        if '_rev' not in doc:
            raise RevisionError("missing '_rev' item in the document")
        if '_id' not in doc:
            raise ExistenceError("missing '_id' item in the document")
        response = self.server._delete(self.name, doc['_id'], 
                                       params={'rev': doc['_rev']})
        if response.status_code in (200, 202):
            data = response.json()
            if not data.get('ok'):
                raise OtherError('response not OK')
        elif response.status_code == 400:
            raise OtherError('invalid request body or parameters')
        elif response.status_code == 401:
            raise AuthorizationError('write privilege required')
        elif response.status_code == 404:
            raise ExistenceError('no such document')
        elif response.status_code == 409:
            raise RevisionError("missing or incorrect '_rev' item")
        else:
            raise OtherError("{r.status_code} {r.reason}".format(r=response))

    def destroy(self):
        """Delete this database and all its contents.
        Raises AuthorizationError if not server admin privileges.
        Raises ExistenceError if no such database.
        Raises OtherError if there is some other error.
        """
        response = self.server._delete(self.name)
        if response.status_code in (200, 202):
            del self.name       # Make this instance unusable.
        elif response.status_code == 401:
            raise AuthorizationError('server admin privileges required')
        elif response.status_code in (400, 404):
            raise ExistenceError('no such database')
        else:
            raise OtherError("{r.status_code} {r.reason}".format(r=response))
