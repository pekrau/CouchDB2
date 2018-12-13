# -*- coding: utf-8 -*-
"""Python module to interface with CouchDB (version 2).
Relies on requests, http://docs.python-requests.org/en/master/
"""

from __future__ import print_function, unicode_literals

import collections
import json
import re
import uuid

import requests

__version__ = '0.0.1'

JSON_MIME = 'application/json'

class CouchDB2Exception(Exception):
    "Base CouchDB2 exception class."

class ConnectionError(CouchDB2Exception):
    "Could not connect the the CouchDB server."

class RevisionError(CouchDB2Exception):
    "Wrong or missing '_rev' item in the document to save."

class AuthorizationError(CouchDB2Exception):
    "Current user not authorized to perform the operation."


class Server(object):
    "Connection to the CouchDB server."

    def __init__(self, href='http://localhost:5984/',
                 username=None, password=None):
        self.href = href.rstrip('/') + '/'
        self.username = username
        self.password = password
        self._requests_session = requests.Session()
        if self.username and self.password:
            self._requests_session.auth = (self.username, self.password)
        self._requests_session.headers.update({'Accept': JSON_MIME})
        data = self()
        self.version = data['version']

    def __str__(self):
        return "CouchDB {s.version} {s.href}".format(s=self)

    def __repr__(self):
        return "couchdb2.Server({s.href})".format(s=self)

    def __len__(self):
        "Number of non-system databases."
        data = self('_all_dbs')
        return len([n for n in data if not n.startswith('_')])

    def __call__(self, path=None):
        href = self.href
        if path:
            href += path
        response = self._requests_session.get(href)
        if not response.ok:
            response.raise_for_status()
        return response.json(object_pairs_hook=collections.OrderedDict)

    def __iter__(self):
        "Iterate over all non-system databases on the server."
        data = self('_all_dbs')
        return iter([Database(self, n) for n in data if not n.startswith('_')])

    def __getitem__(self, name):
        "Get the named database."
        try:
            return Database(self, name)
        except requests.HTTPError as error:
            if error.response.status_code == 404:
                raise KeyError('no such database')
            else:
                raise

    def __contains__(self, name):
        "Does the named database exist?"
        href = self.href + name
        response = self._requests_session.head(href)
        return response.status_code == 200

    def __delitem__(self, name):
        """Delete the named database.
        Raises ValueError if the name is invalid.
        Raises AuthorizationError if not server admin privileges.
        Raises KeyError if no such database.
        """
        href = self.href + name
        response = self._requests_session.delete(href)
        if response.status_code in (200, 202):
            return
        elif response.status_code == 400:
            raise ValueError("invalid database name {}".format(name))
        elif response.status_code == 401:
            raise AuthorizationError('server admin privileges required')
        elif response.status_code == 404:
            raise KeyError('no such database')

    def create(self, name):
        """Create the named database.
        Raises ValueError if the name is invalid.
        Raises AuthorizationError if not server admin privileges.
        Raises KeyError if a database with that name already exists.
        """
        href = self.href + name
        response = self._requests_session.put(href)
        if response.status_code in (201, 202):
            return Database(self, name)
        elif response.status_code == 400:
            raise ValueError("invalid database name {}".format(name))
        elif response.status_code == 401:
            raise AuthorizationError('server admin privileges required')
        elif response.status_code == 412:
            raise KeyError('database already exists')


class Database(object):
    "Interface to a CouchDB database."

    def __init__(self, server, name):
        "Raises ValueError if the name is invalid."
        self.server = server
        self.name = name
        self.info = self.server(name)
        self.href = self.server.href + name

    def __str__(self):
        return "CouchDB database {s.name}".format(s=self)

    def __repr__(self):
        return "couchdb2.Database({s.server!r}, {s.name})".format(s=self)

    def __len__(self):
        "Return the number of documents."
        return self.info['doc_count']

    def __contains__(self, id):
        "Does a document with the given id exist in the database?"
        href = self.href + '/' + id
        response = self._requests_session.head(href)
        if response.status_code == 401:
            raise AuthorizationError('read privilege required')
        return response.status_code in (200, 304)

    def __getitem__(self, id):
        raise NotImplementedError

    def __delitem__(self, id):
        raise NotImplementedError

    def save(self, doc):
        """Insert or update the document. 
        If there is an item '_rev' in the document, then update it,
        else insert it.
        If the document does not contain an item '_id', it is created 
        and given a UUID4 value.
        Raises RevisionError if the '_rev' item does not match.
        """
        if '_rev' in doc:
            self.update(doc)
        else:
            self.insert(doc)

    def insert(self, doc):
        """Insert the document.
        If the document does not contain an item '_id', it is created 
        and given a UUID4 value.
        Raises KeyError if the document already exists.
        Raises ValueError if something else went wrong.
        Raises AuthorizationError if not privileged to write.
        """
        if not '_id' in doc:
            doc['_id'] = uuid.uuid4().hex
        response = self.server._requests_session.post(self.href, json=doc)
        if response.status_code in (201, 202):
            data = response.json()
            if not data.get('ok'):
                raise ValueError('response not OK')
            return (data['id'], data['rev'])
        elif response.status_code == 409:
            raise KeyError('document id already exists')
        elif response.status_code == 401:
            raise AuthorizationError('write privilege required')
        else:
            raise ValueError("{r.status_code} {r.reason}".format(r=response))

    def update(self, doc):
        """Update the document.
        """
        raise NotImplementedError


if __name__ == '__main__':
    server = Server()
    print(server)
    print(repr(server))
    dbs = list(server)
    print(dbs)
    print('beerclub' in server)
    print('blah' in server)
    beerclub = server['beerclub']
    print(beerclub)
    bla = server['bla']
