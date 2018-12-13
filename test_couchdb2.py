# -*- coding: utf-8 -*-
"""Test the Python module to interface with CouchDB (version 2).
Uses py.test.
"""

from __future__ import print_function, unicode_literals

from collections import OrderedDict

import pytest

import couchdb2


def test_server():
    server = couchdb2.Server()
    assert server.version.split('.')[0] == '2'

def test_database_creation():
    server = couchdb2.Server()
    count = len(server)
    name = 'my_test_database'
    assert name not in server
    with pytest.raises(KeyError):
        server[name]
    new = server.create(name)
    assert name in server
    assert len(server) == count + 1
    with pytest.raises(KeyError):
        another = server.create(name)
    del server[name]
    assert name not in server
    with pytest.raises(KeyError):
        del server[name]
    assert len(server) == count

def test_document_insert_delete():
    server = couchdb2.Server()
    db = server.create('my_test_database')
    doc1 = OrderedDict()
    doc1['name'] = 'thingy'
    doc1['age'] = 1
    db.insert(doc1)
    del server[db.name]
