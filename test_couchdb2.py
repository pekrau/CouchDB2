"""Test the Python interface module to CouchDB v2.x.
Uses package py.test.
"""

from __future__ import print_function

import pytest

import couchdb2


DBNAME = 'pytest'

def test_server():
    server = couchdb2.Server()
    # Verified connection
    assert server.version
    # Cannot connect to nonsense address
    with pytest.raises(IOError):
        server = couchdb2.Server('http://localhost:123456/')

def test_database():
    server = couchdb2.Server()
    # Keep track of how many databases to start with
    count = len(server)
    # Make sure the test database is not present
    assert DBNAME not in server
    with pytest.raises(couchdb2.NotFoundError):
        db = server[DBNAME]
    # Create the test database, and check for it
    db = server.create(DBNAME)
    assert DBNAME in server
    assert len(server) == count + 1
    # Fail to create it again
    with pytest.raises(couchdb2.CreationError):
        another = couchdb2.Database(server, DBNAME, check=False).create()
    # Destroy the database, and check that it went away
    db.destroy()
    assert not db.exists()
    assert DBNAME not in server
    with pytest.raises(couchdb2.NotFoundError):
        db = server[DBNAME]
    assert len(server) == count

def test_document_create():
    db = couchdb2.Server().create(DBNAME)
    # Empty database
    assert len(db) == 0
    # Store a document with predefined id
    docid = 'hardwired id'
    doc = {'_id': docid, 'name': 'thingy', 'age': 1}
    doc1 = doc.copy()
    db.put(doc1)
    # Remember keys of original document
    keys1 = set(db.get(docid).keys())
    assert len(db) == 1
    # Exact copy of original document, except no '_rev'
    doc2 = doc.copy()
    with pytest.raises(couchdb2.RevisionError):
        db.put(doc2)
    # Delete the document, check it went away
    db.delete(doc1)
    assert doc1['_id'] not in db
    assert len(db) == 0
    db.put(doc2)
    # Keys of this document must be equal to keys of original
    keys2 = set(db.get(docid).keys())
    assert keys1 == keys2
    # A single document in database
    assert len(db) == 1
    db.destroy()

def test_document_update():
    server = couchdb2.Server()
    db = server.create(DBNAME)
    assert len(db) == 0
    doc = {'name': 'Per', 'age': 59, 'mood': 'jolly'}
    # Store first revision of document
    doc1 = doc.copy()
    db.put(doc1)
    id1 = doc1['_id']
    rev1 = doc1['_rev']
    assert len(db) == 1
    # Store second revision of document
    doc2 = doc1.copy()
    doc2['mood'] = 'excellent'
    db.put(doc2)
    # Get the second revision
    doc3 = db[id1]
    assert doc3['mood'] == 'excellent'
    rev2 = doc3['_rev']
    assert rev1 != rev2
    # Get the first revision
    doc1_copy = db.get(id1, rev=rev1)
    assert doc1_copy == doc1
    db.destroy()

def test_design_view():
    db = couchdb2.Server().create(DBNAME)
    db.put_design('docs',
                  {'views':
                   {'name':
                    {'map': "function (doc) {if (doc.name===undefined) return;"
                            " emit(doc.name, null);}"},
                    'name_sum':
                    {'map': "function (doc) {emit(doc.name, doc.number);}",
                     'reduce': '_sum'},
                    'name_count':
                    {'map': "function (doc) {emit(doc.name, null);}",
                     'reduce': '_count'}
                   }})
    doc = {'name': 'mine', 'number': 2}
    db.put(doc)
    # Get all rows: one single
    result = db.view('docs', 'name')
    assert len(result.rows) == 1
    assert result.total_rows == 1
    row = result.rows[0]
    assert row.key == 'mine'
    assert row.value is None
    assert row.doc is None
    # Get all rows, with documents
    result = db.view('docs', 'name', include_docs=True)
    assert len(result.rows) == 1
    assert result.total_rows == 1
    row = result.rows[0]
    assert row.key == 'mine'
    assert row.value is None
    assert row.doc == doc
    # Store another document
    doc = {'name': 'another', 'number': 3}
    db.put(doc)
    result = db.view('docs', 'name_sum')
    # Sum of all item in all documents; 1 row having no document
    assert len(result.rows) == 1
    assert result.rows[0].doc is None
    assert result.rows[0].value == 5
    # Count all documents
    result = db.view('docs', 'name_count')
    assert len(result.rows) == 1
    assert result.rows[0].value == 2
    # No key 'name' in document; not included in index
    db.put({'number': 8})
    result = db.view('docs', 'name')
    assert len(result.rows) == 2
    assert result.total_rows == 2
    db.destroy()

def test_iterator():
    db = couchdb2.Server().create(DBNAME)
    orig = {'field': 'data'}
    # One more than chunk size to test paging
    N = couchdb2.Database.CHUNK_SIZE + 1
    docs = {}
    for n in range(N):
        doc = orig.copy()
        doc['n'] = n
        db.put(doc)
        docs[doc['_id']] = doc
    assert len(db) == N
    assert docs == dict([(d['_id'], d) for d in db])
    db.destroy()

def test_index():
    "Mango index; only for CouchDB version 2 and higher."
    server = couchdb2.Server()
    if not server.version.startswith('2'): return
    db = server.create(DBNAME)
    db.put({'name': 'Per', 'type': 'person', 'content': 'stuff'})
    db.put({'name': 'Anders', 'type': 'person', 'content': 'other stuff'})
    db.put({'name': 'Per', 'type': 'computer', 'content': 'data'})
    # Find without index
    result = db.find({'type': 'person'})
    assert len(result['docs']) == 2
    assert result.get('warning')
    result = db.find({'type': 'computer'})
    assert len(result['docs']) == 1
    result = db.find({'type': 'house'})
    assert len(result['docs']) == 0
    # Index for 'name' item
    db.load_index(['name'])
    result = db.find({'name': 'Per'})
    assert len(result['docs']) == 2
    assert not result.get('warning')
    # Load an index having a partial filter selector
    ddocname = 'mango'
    indexname = 'myindex'
    result = db.load_index(['name'], ddoc=ddocname, name=indexname,
                           selector={'type': 'person'})
    assert result['id'] == "_design/{}".format(ddocname)
    assert result['name'] == indexname
    result = db.find({'type': 'person'})
    # Does not use an index; warns about that
    assert result.get('warning')
    assert len(result['docs']) == 2
    # Use an explicit selection
    result = db.find({'name': 'Per', 'type': 'person'})
    assert len(result['docs']) == 1
    # Same as above, implicit selection via partial index
    result = db.find({'name': 'Per'}, use_index=[ddocname, indexname])
    assert len(result['docs']) == 1
    assert not result.get('warning')
    db.destroy()

def test_document_attachments():
    db = couchdb2.Server().create(DBNAME)
    id = 'mydoc'
    doc = {'_id': id, 'name': 'myfile', 'contents': 'a Python file'}
    db.put(doc)
    # Load this file's contents as attachment
    with open(__file__, 'rb') as infile:
        db.put_attachment(doc, infile)
    doc = db[id]
    attfile = db.get_attachment(doc, __file__)
    # Check this file's contents against attachment
    with open(__file__, 'rb') as infile:
        assert attfile.read() == infile.read()
    db.destroy()
