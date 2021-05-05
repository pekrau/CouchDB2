"Test the Python interface module to CouchDB v2.x."

# Standard packages
import os
import tempfile
import unittest

import couchdb2


class Test(unittest.TestCase):

    def setUp(self):
        self.settings = {"SERVER": "http://localhost:5984",
                         "DATABASE": "test",
                         "USERNAME": None,
                         "PASSWORD": None}
        try:
            self.settings = couchdb2.read_settings("test_settings.json",
                                                   self.settings)
        except IOError:
            pass
        self.server = couchdb2.Server(username=self.settings["USERNAME"],
                                      password=self.settings["PASSWORD"])

    def tearDown(self):
        db = self.server.get(self.settings["DATABASE"], check=False)
        if db.exists():
            db.destroy()

    def test_00_no_such_server(self):
        "Check the error when trying to use a non-existent server."
        with self.assertRaises(IOError):
            couchdb2.Server("http://localhost:123456/").version

    def test_01_server(self):
        "Test the basic data for the server."
        data = self.server()
        self.assertIn("version", data)
        self.assertIn("vendor", data)
        self.assertIsNotNone(self.server.version)
        if self.server.version >= "2.0":
            self.assertTrue(self.server.up())
            data = self.server.get_config()
            self.assertIn("log", data)
            self.assertIn("vendor", data)
            data = self.server.get_cluster_setup()
            self.assertIn("state", data)
            data = self.server.get_membership()
            self.assertIn("all_nodes", data)
            self.assertIn("cluster_nodes", data)
            data = self.server.get_scheduler_jobs()
            self.assertIn("jobs", data)
            data = self.server.get_node_stats()
            self.assertIn("couchdb", data)
            self.assertIn("httpd", data["couchdb"])
            data = self.server.get_node_system()
            self.assertIn("uptime", data)
            self.assertIn("memory", data)
            data = self.server.get_active_tasks()
            self.assertTrue(isinstance(data, type([])))

    def test_02_no_database(self):
        "Check that the specified database does not exist."
        self.assertNotIn(self.settings["DATABASE"], self.server)
        with self.assertRaises(couchdb2.NotFoundError):
            db = self.server[self.settings["DATABASE"]]

    def test_03_database(self):
        "Test database creation and destruction."
        # How many databases at start?
        count = len(self.server)
        # Create a database.
        db = self.server.create(self.settings["DATABASE"])
        self.assertIn(self.settings["DATABASE"], self.server)
        self.assertEqual(len(self.server), count+1)
        # Fail to create the database again.
        with self.assertRaises(couchdb2.CreationError):
            another = self.server.create(self.settings["DATABASE"])
        # Destroy the recently created database.
        db.destroy()
        self.assertFalse(db.exists())
        self.assertNotIn(self.settings["DATABASE"], self.server)
        with self.assertRaises(couchdb2.NotFoundError):
            db = self.server[self.settings["DATABASE"]]
        self.assertEqual(len(self.server), count)

    def test_04_document_create(self):
        "Test creating a document in an empty database."
        db = self.server.create(self.settings["DATABASE"])
        self.assertEqual(len(db), 0)
        # Store a document with predefined id
        docid = "hardwired id"
        doc = {"_id": docid, "name": "thingy", "age": 1}
        doc1 = doc.copy()
        db.put(doc1)
        self.assertEqual(len(db), 1)
        self.assertIn("_rev", doc1)
        keys = set(db.get(docid).keys())
        # Exact copy of original document."
        doc2 = doc.copy()
        # Cannot update existing document without "_rev".
        with self.assertRaises(couchdb2.RevisionError):
            db.put(doc2)
        # Delete the document.
        db.delete(doc1)
        self.assertNotIn(doc1["_id"], db)
        self.assertEqual(len(db), 0)
        # Add document again.
        db.put(doc2)
        # Keys of this new document must be equal to keys of original
        keys2 = set(db.get(docid).keys())
        self.assertEqual(keys, keys2)
        # A single document in database.
        self.assertEqual(len(db), 1)

    def test_05_document_update(self):
        "Test updating a document."
        db = self.server.create(self.settings["DATABASE"])
        self.assertEqual(len(db), 0)
        doc = {"name": "Per", "age": 61, "mood": "jolly"}
        # Store the first revision of document.
        doc1 = doc.copy()
        db.put(doc1)
        self.assertIn("_rev", doc1)
        id1 = doc1["_id"]
        rev1 = doc1["_rev"]
        self.assertEqual(len(db), 1)
        # Store a revised version of the document.
        doc2 = doc1.copy()
        doc2["mood"] = "excellent"
        db.put(doc2)
        self.assertIn("_rev", doc2)
        # Get a new copy of the second revision.
        doc3 = db[id1]
        self.assertEqual(doc3["mood"], "excellent")
        self.assertNotEqual(doc3["_rev"], rev1)
        # Get a new copy of the first revision.
        doc1_copy = db.get(id1, rev=rev1)
        self.assertEqual(doc1_copy, doc1)

    def test_06_design_view(self):
        "Test design views containing reduce and count functions."
        db = self.server.create(self.settings["DATABASE"])
        self.assertEqual(len(db), 0)
        db.put_design("docs",
                      {"views":
                       {"name":
                        {"map": "function (doc) {if (doc.name===undefined) return;"
                                " emit(doc.name, null);}"},
                        "name_sum":
                        {"map": "function (doc) {emit(doc.name, doc.number);}",
                         "reduce": "_sum"},
                        "name_count":
                        {"map": "function (doc) {emit(doc.name, null);}",
                         "reduce": "_count"}
                       }})
        doc = {"name": "mine", "number": 2}
        db.put(doc)
        # Get all rows without documents: one single in result.
        result = db.view("docs", "name")
        self.assertEqual(len(result.rows), 1)
        self.assertEqual(result.total_rows, 1)
        row = result.rows[0]
        self.assertEqual(row.key, "mine")
        self.assertIsNone(row.value)
        self.assertIsNone(row.doc)
        # Get all rows, with documents.
        result = db.view("docs", "name", include_docs=True)
        self.assertEqual(len(result.rows), 1)
        self.assertEqual(result.total_rows, 1)
        row = result.rows[0]
        self.assertEqual(row.key, "mine")
        self.assertIsNone(row.value)
        self.assertEqual(row.doc, doc)
        # Store another document.
        doc = {"name": "another", "number": 3}
        db.put(doc)
        # Sum of values of all fields 'number' in all documents;
        # 1 row having no document.
        result = db.view("docs", "name_sum")
        self.assertEqual(len(result.rows), 1)
        self.assertIsNone(result.rows[0].doc)
        self.assertEqual(result.rows[0].value, 5)
        # Count all documents.
        result = db.view("docs", "name_count")
        self.assertEqual(len(result.rows), 1)
        self.assertEqual(result.rows[0].value, 2)
        # No key "name" in document; not included in index.
        db.put({"number": 8})
        result = db.view("docs", "name")
        self.assertEqual(len(result.rows), 2)
        self.assertEqual(result.total_rows, 2)

    def test_07_iterator(self):
        "Test database iterator over all documents."
        db = self.server.create(self.settings["DATABASE"])
        orig = {"field": "data"}
        # One more document than chunk size to test paging.
        N = couchdb2.CHUNK_SIZE + 1
        docs = {}
        for n in range(N):
            doc = orig.copy()
            doc["n"] = n
            db.put(doc)
            docs[doc["_id"]] = doc
        self.assertEqual(len(db), N)
        docs_from_iterator = dict([(d["_id"], d) for d in db])
        self.assertEqual(docs, docs_from_iterator)
        self.assertEqual(set(docs.keys()), set(db.ids()))

    def test_08_index(self):
        """Test the Mango index feature.
        Requires CouchDB server version 2 and later.
        """
        if not self.server.version >= "2.0": return
        db = self.server.create(self.settings["DATABASE"])
        self.assertEqual(len(db), 0)
        db.put({"name": "Per", "type": "person", "content": "stuff"})
        db.put({"name": "Anders", "type": "person", "content": "other stuff"})
        db.put({"name": "Per", "type": "computer", "content": "data"})
        # Find document without index; generates a warning.
        result = db.find({"type": "person"})
        self.assertEqual(len(result["docs"]), 2)
        self.assertIsNotNone(result.get("warning"))
        result = db.find({"type": "computer"})
        self.assertEqual(len(result["docs"]), 1)
        result = db.find({"type": "house"})
        self.assertEqual(len(result["docs"]), 0)
        # Add an index for "name" item.
        db.put_index(["name"])
        result = db.find({"name": "Per"})
        self.assertEqual(len(result["docs"]), 2)
        self.assertIsNone(result.get("warning"))
        # Add an index having a partial filter selector.
        ddocname = "mango"
        indexname = "myindex"
        result = db.put_index(["name"], ddoc=ddocname, name=indexname,
                              selector={"type": "person"})
        self.assertEqual(result["id"], "_design/{}".format(ddocname))
        self.assertEqual(result["name"], indexname)
        # Search does not use an index; warns about that.
        result = db.find({"type": "person"})
        self.assertIsNotNone(result.get("warning"))
        self.assertEqual(len(result["docs"]), 2)
        # Search using an explicit selection.
        result = db.find({"name": "Per", "type": "person"})
        self.assertEqual(len(result["docs"]), 1)
        # Same as above, implicit selection via partial index.
        result = db.find({"name": "Per"}, use_index=[ddocname, indexname])
        self.assertEqual(len(result["docs"]), 1)
        self.assertIsNone(result.get("warning"))

    def test_09_document_attachments(self):
        "Test adding a file as attachment to a document."
        db = self.server.create(self.settings["DATABASE"])
        id = "mydoc"
        doc = {"_id": id, "name": "myfile", "contents": "a Python file"}
        db.put(doc)
        rev1 = doc["_rev"]
        # Store this Python file's contents as an attachment.
        with open(__file__, "rb") as infile:
            rev2 = db.put_attachment(doc, infile)
        self.assertNotEqual(rev1, rev2)
        self.assertEqual(doc["_rev"], rev2)
        # Check this Python file's contents against the attachment.
        attfile = db.get_attachment(doc, __file__)
        attdata = attfile.read()
        with open(__file__, "rb") as infile:
            data = infile.read()
        self.assertEqual(attdata, data)

    def test_10_dump(self):
        "Test dumping the contents of a database into a file."
        db = self.server.create(self.settings["DATABASE"])
        id1 = "mydoc"
        name1 = "myfile"
        doc1 = {"_id": id1, "name": name1, "contents": "a Python file"}
        db.put(doc1)
        # Store this Python file's contents as an attachment.
        with open(__file__, "rb") as infile:
            db.put_attachment(doc1, infile)
        # Check that non-ASCII characters work in the file.
        id2 = u"åöä"
        name2 = u"Åkersjöö"
        doc2 = {"_id": id2, "name": name2, "contents": "ũber unter vor"}
        db.put(doc2)
        # Get a filename to dump to.
        f = tempfile.NamedTemporaryFile(delete=False)
        filepath = f.name
        f.close()
        # Actually dump the database data, and then destroy the database.
        counts1 = db.dump(filepath)
        db.destroy()
        self.assertFalse(db.exists())
        # Create the database again, and undump the data.
        db = self.server.create(self.settings["DATABASE"])
        counts2 = db.undump(filepath)
        os.unlink(filepath)
        self.assertEqual(counts1, counts2)
        # Compare the contents of the undumped database with the sources.
        doc1 = db[id1]
        self.assertEqual(doc1["name"], name1)
        with open(__file__, "rb") as infile:
            file_content = infile.read()
        attachment_content = db.get_attachment(doc1, __file__).read()
        self.assertEqual(file_content, attachment_content)
        doc2 = db[id2]
        self.assertEqual(doc2["name"], name2)


if __name__ == "__main__":
    unittest.main()
