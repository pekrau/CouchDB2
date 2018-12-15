# -*- coding: utf-8 -*-
"""Test the Python interface module to CouchDB (version 2).
Uses py.test.
"""

from __future__ import print_function, unicode_literals

import pytest

import couchdb2


def test_server():
    server = couchdb2.Server()
    assert server.version
    with pytest.raises(IOError):
        server = couchdb2.Server('http://localhost:123456/')

def test_database():
    server = couchdb2.Server()
    count = len(server)
    name = 'mytest'
    assert name not in server
    with pytest.raises(couchdb2.NotFoundError):
        db = server[name]
    db = server.get(name)
    assert name in server
    assert len(server) == count + 1
    with pytest.raises(couchdb2.CreationError):
        another = couchdb2.Database(server, name, check=False).create()
    db.destroy()
    assert not db.exists()
    assert name not in server
    with pytest.raises(couchdb2.NotFoundError):
        db = server[name]
    assert len(server) == count

def test_document_create():
    db = couchdb2.Server().get('mytest')
    assert len(db) == 0
    doc = {}
    docid = 'hardwired id'
    doc['_id'] = docid
    doc['name'] = 'thingy'
    doc['age'] = 1
    doc1 = doc.copy()
    db.save(doc1)
    keys1 = set(db.get(docid).keys())
    assert len(db) == 1
    doc2 = doc.copy()
    with pytest.raises(couchdb2.RevisionError):
        db.save(doc2)
    db.delete(doc1)
    assert doc1['_id'] not in db
    assert len(db) == 0
    db.save(doc2)
    keys2 = set(db.get(docid).keys())
    assert keys1 == keys2
    assert len(db) == 1
    db.destroy()

def test_document_update():
    db = couchdb2.Server().get('mytest')
    assert len(db) == 0
    doc = {'name': 'Per', 'age': 59, 'mood': 'OK'}
    doc1 = doc.copy()
    db.save(doc1)
    assert len(db) == 1
    docid = doc1.get('_id')
    assert docid
    doc1['mood'] = 'excellent'
    db.save(doc1)
    doc2 = db[docid]
    assert doc2['mood'] == 'excellent'
    doc3 = doc.copy()
    db.save(doc3)
    assert len(db) == 2
    assert docid != doc3['_id']
    db.destroy()

def test_design_view():
    db = couchdb2.Server().get('mytest')
    db.load_design('all',
                   {'views':
                    {'name':
                     {'map': "function (doc) {emit(doc.name, null);}"}}})
    id = 'mydoc'
    doc = {'_id': id, 'name': 'mine', 'contents': 'some stuff'}
    db.save(doc)
    result = db.view('all', 'name')
    assert len(result.rows) == 1
    assert result.total_rows == 1
    row = result.rows[0]
    assert row.key == 'mine'
    assert row.value is None
    assert row.doc is None
    result = db.view('all', 'name', include_docs=True)
    assert len(result.rows) == 1
    assert result.total_rows == 1
    row = result.rows[0]
    assert row.key == 'mine'
    assert row.value is None
    assert row.doc == doc
    db.destroy()

def test_document_attachments():
    db = couchdb2.Server().get('mytest')
    id = 'mydoc'
    doc = {'_id': id, 'name': 'myfile', 'contents': 'a Python file'}
    db.save(doc)
    with open(__file__, 'rb') as infile:
        db.put_attachment(doc, infile)
    doc = db[id]
    attfile = db.get_attachment(doc, __file__)
    with open(__file__, 'rb') as infile:
        assert attfile.read() == infile.read()
    db.destroy()
