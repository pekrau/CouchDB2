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
    assert server.version
    with pytest.raises(IOError):
        server = couchdb2.Server('http://localhost:123456/')

def test_database_creation():
    server = couchdb2.Server()
    count = len(server)
    name = 'my_test_database'
    assert name not in server
    with pytest.raises(KeyError):
        server[name]
    db = server.create(name)
    assert name in server
    assert len(server) == count + 1
    with pytest.raises(couchdb2.CreationError):
        another = server.create(name)
    db.destroy()
    assert name not in server
    with pytest.raises(couchdb2.ExistenceError):
        db = server[name]
    assert len(server) == count

def test_document_insert_delete():
    server = couchdb2.Server()
    db = server.create('my_test_database')
    assert len(db) == 0
    doc = OrderedDict()
    docid = 'hardwired id'
    doc['_id'] = docid
    doc['name'] = 'thingy'
    doc['age'] = 1
    doc1 = doc.copy()
    db.insert(doc1)
    keys1 = db.get(docid).keys()
    assert len(db) == 1
    doc2 = doc.copy()
    with pytest.raises(couchdb2.CreationError):
        db.insert(doc2)
    del db[doc1['_id']]
    assert doc1['_id'] not in db
    assert len(db) == 0
    db.insert(doc2)
    keys2 = db.get(docid).keys()
    assert keys1 == keys2
    assert len(db) == 1
    db.destroy()

def test_document_update():
    server = couchdb2.Server()
    db = server.create('my_test_database')
    assert len(db) == 0
    doc = {'name': 'Per', 'age': 59, 'mood': 'OK'}
    doc1 = doc.copy()
    db.save(doc1)
    assert len(db) == 1
    docid = doc1.get('_id')
    assert docid
    doc1['mood'] = 'excellent'
    db.update(doc1)
    doc2 = db[docid]
    assert doc2['mood'] == 'excellent'
    doc3 = doc.copy()
    docid3, rev = db.insert(doc3)
    assert len(db) == 2
    assert docid != docid3
    db.destroy()
