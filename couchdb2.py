"""CouchDB v2.x Python 3 interface in a single module. Also a command line tool.

Most, but not all, features of this module work with CouchDB version < 2.0.

Relies on 'requests': http://docs.python-requests.org/en/master/
"""

__version__ = "1.11.0"

# Standard packages
import argparse
import collections
import getpass
import gzip
import io
import json
import mimetypes
import os
import os.path
import tarfile
import sys
import time
import uuid

if sys.version_info[:2] < (3, 7):
    from collections import OrderedDict as dict

# Third-party package: https://docs.python-requests.org/en/master/
import requests

JSON_MIME = "application/json"
BIN_MIME = "application/octet-stream"
CHUNK_SIZE = 100


class Server:
    "An instance of the class is a connection to the CouchDB server."

    def __init__(self, href="http://localhost:5984/",
                 username=None, password=None, use_session=True, ca_file=None):
        """An instance of the class is a connection to the CouchDB server.

        - `href` is the URL to the CouchDB server itself.
        - `username` and `password` specify the CouchDB user account to use.
        - If `use_session` is `True`, then an authenticated session is used
          transparently. Otherwise, the values of `username` and `password` are
          sent with each request.
        - `ca_file` is a path to a file or a directory containing CAs if
          you need to access databases in HTTPS.
        """
        self.href = href.rstrip("/") + "/"
        self._session = requests.Session()
        self._session.headers.update({"Accept": JSON_MIME})
        if ca_file is not None:
            self._session.verify = ca_file
        if username and password:
            if use_session:
                self._POST("_session",
                           data={"name": username, "password": password})
            else:
                self._session.auth = (username, password)

    @property
    def version(self):
        "Returns the version of the CouchDB server software."
        try:
            return self._version
        except AttributeError:
            self._version = self._GET().json()["version"]
            return self._version

    @property
    def user_context(self):
        "Returns the user context of the connection."
        response = self._GET("_session")
        return response.json()

    def __str__(self):
        "Returns a simple string representation of the server interface."
        return f"CouchDB {self.version} {self.href}"

    def __len__(self):
        "Returns the number of user-defined databases."
        data = self._GET("_all_dbs").json()
        return len([n for n in data if not n.startswith("_")])

    def __iter__(self):
        "Returns an iterator over all user-defined databases on the server."
        data = self._GET("_all_dbs").json()
        return iter([Database(self, n, check=False)
                     for n in data if not n.startswith("_")])

    def __getitem__(self, name):
        "Gets the named database."
        return Database(self, name, check=True)

    def __contains__(self, name):
        "Does the named database exist?"
        response = self._HEAD(name, errors={404: None})
        return response.status_code == 200

    def __call__(self):
        "Returns meta information about the instance."
        response = self._GET()
        return response.json()

    def __del__(self):
        "Clean-up: Close the 'requests' session."
        self._session.close()

    def up(self):
        """Is the server up and running, ready to respond to requests?
        Returns a boolean.

        CouchDB version >= 2.0
        """
        assert self.version >= "2.0"
        response = self._session.get(self.href + "_up")
        return response.status_code == 200

    def get(self, name, check=True):
        """Gets the named database. Returns an instance of class `Database`.

        Raises `NotFoundError` if `check` is `True` and the database
        does not exist.
        """
        return Database(self, name, check=check)

    def create(self, name, n=3, q=8, partitioned=False):
        """Creates the named database. Raises `CreationError` if it
        already exists.

        - `name`: The name of the database.
        - `n`: The number of replicas.
        - `q`: The number of shards.
        - `partitioned`: Whether to create a partitioned database.
        """
        db = Database(self, name, check=False)
        return db.create(n=n, q=q, partitioned=partitioned)

    def get_config(self, nodename="_local"):
        "Gets the named node's configuration."
        response = self._GET("_node", nodename, "_config")
        return response.json()

    def get_active_tasks(self):
        "Returns a list of running tasks."
        response = self._GET("_active_tasks")
        return response.json()

    def get_cluster_setup(self, ensure_dbs_exists=None):
        """Returns the status of the node or cluster.

        `ensure_dbs_exists` is a list system databases to ensure exist on the
        node/cluster. Defaults to `["_users","_replicator"]`.

        CouchDB version >= 2.0
        """
        assert self.version >= "2.0"
        if ensure_dbs_exists is None:
            params = {}
        else:
            params = {"ensure_dbs_exists": ensure_dbs_exists}
        response = self._GET("_cluster_setup", params=params)
        return response.json()

    def set_cluster_setup(self, doc):
        """Configures a node as a single node, as part of a cluster,
        or finalize a cluster.

        See the CouchDB documentation for the contents of `doc`.

        CouchDB version >= 2.0
        """
        assert self.version >= "2.0"
        self._POST("_cluster_setup", json=doc)

    def get_membership(self):
        """Returns data about the nodes that are part of the cluster.

        CouchDB version >= 2.0
        """
        assert self.version >= "2.0"
        response = self._GET("_membership")
        return response.json()

    def set_replicate(self, doc):
        """Request, configure, or stop, a replication operation.

        See the CouchDB documentation for the contents of `doc`.
        """
        response = self._POST("_replicate", json=doc)
        return response.json()

    def get_scheduler_jobs(self, limit=None, skip=None):
        """Gets a list of replication jobs.

        - 'limit': How many results to return.
        - 'skip': How many result to skip starting at the beginning,
          ordered by replication ID.
        """
        params = {}
        if limit is not None:
            params["limit"] = _jsons(limit)
        if skip is not None:
            params["skip"] = _jsons(skip)
        response = self._GET("_scheduler/jobs", params=params)
        return response.json()

    def get_scheduler_docs(self, limit=None, skip=None):
        """Gets information about replication document(s).

        - 'limit': How many results to return.
        - 'skip': How many result to skip starting at the beginning,
          ordered by document ID.
        """
        params = {}
        if limit is not None:
            params["limit"] = _jsons(limit)
        if skip is not None:
            params["skip"] = _jsons(skip)
        response = self._GET("_scheduler/docs", params=params)
        return response.json()

    def get_node_stats(self, nodename="_local"):
        "Returns statistics for the running server."
        response = self._GET("_node", nodename, "_stats")
        return response.json()

    def get_node_system(self, nodename="_local"):
        "Returns various system-level statistics for the running server."
        response = self._GET("_node", nodename, "_system")
        return response.json()

    def _HEAD(self, *segments, **kwargs):
        "HTTP HEAD request to the CouchDB server, and check the response."
        response = self._session.head(self._href(segments))
        self._check(response, errors=kwargs.get("errors", {}))
        return response

    def _GET(self, *segments, **kwargs):
        "HTTP GET request to the CouchDB server, and check the response."
        kw = self._kwargs(kwargs, "headers", "params")
        response = self._session.get(self._href(segments), **kw)
        self._check(response, errors=kwargs.get("errors", {}))
        return response

    def _PUT(self, *segments, **kwargs):
        "HTTP PUT request to the CouchDB server, and check the response."
        kw = self._kwargs(kwargs, "json", "data", "headers")
        response = self._session.put(self._href(segments), **kw)
        self._check(response, errors=kwargs.get("errors", {}))
        return response

    def _POST(self, *segments, **kwargs):
        "HTTP POST request to the CouchDB server, and check the response."
        kw = self._kwargs(kwargs, "json", "data", "headers", "params")
        response = self._session.post(self._href(segments), **kw)
        self._check(response, errors=kwargs.get("errors", {}))
        return response

    def _DELETE(self, *segments, **kwargs):
        """HTTP DELETE request to the CouchDB server, and check the response.
        Pass parameters in the keyword argument 'params'.
        """
        kw = self._kwargs(kwargs, "headers")
        response = self._session.delete(self._href(segments), **kw)
        self._check(response, errors=kwargs.get("errors", {}))
        return response

    def _href(self, segments):
        "Return the complete URL."
        return self.href + "/".join(segments)

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
                raise IOError(f"{response.status_code} {response.reason}")
        if error is not None:
            raise error(response.reason)


class Database:
    "An instance of the class is an interface to a CouchDB database."

    def __init__(self, server, name, check=True):
        self.server = server
        self.name = name
        if check:
            self.check()

    def __str__(self):
        "Returns the name of the CouchDB database."
        return self.name

    def __len__(self):
        "Returns the number of documents in the database."
        return self.server._GET(self.name).json()["doc_count"]

    def __contains__(self, id):
        "Does a document with the given identifier exist in the database?"
        response = self.server._HEAD(self.name, id, errors={404: None})
        return response.status_code in (200, 304)

    def __iter__(self):
        "Returns an iterator over all documents in the database."
        return _DatabaseIterator(self)

    def __getitem__(self, id):
        "Returns the document with the given id."
        result = self.get(id)
        if result is None:
            raise NotFoundError("no such document")
        else:
            return result

    def exists(self):
        "Does the database exist? Return a boolean."
        response = self.server._HEAD(self.name, errors={404: None})
        return response.status_code == 200

    def check(self):
        "Raises 'NotFoundError' if the database does not exist."
        if not self.exists():
            raise NotFoundError(f"Database '{self}' does not exist.")

    def create(self, n=3, q=8, partitioned=False):
        """Creates the database. Raises 'CreationError' if it already exists.

        - `n`: The number of replicas.
        - `q`: The number of shards.
        - `partitioned`: Whether to create a partitioned database.
        """
        self.server._PUT(self.name,
                         data={"n": n, "q": q, "partitioned": partitioned})
        return self

    def destroy(self):
        "Deletes the database and all its contents."
        self.server._DELETE(self.name)

    def get_info(self):
        "Returns a dictionary with information about the database."
        response = self.server._GET(self.name)
        return response.json()

    def get_security(self):
        "Returns a dictionary with security information for the database."
        response = self.server._GET(self.name, "_security")
        return response.json()

    def set_security(self, doc):
        """Sets the security information for the database.

        See the CouchDB documentation for the contents of `doc`.
        """
        self.server._PUT(self.name, "_security", json=doc)

    def compact(self, finish=False, callback=None):
        """Compacts the CouchDB database by rewriting the disk database file
        and removing old revisions of documents.

        - If `finish` is True, then return only when compaction is done.
        - In addition, if defined, the function `callback(seconds)` is called
          every second until compaction is done.
        """
        self.server._POST(self.name, "_compact",
                          headers={"Content-Type": JSON_MIME})
        if finish:
            response = self.server._GET(self.name)
            seconds = 0
            while response.json().get("compact_running"):
                time.sleep(1)
                seconds += 1
                if callback: callback(seconds)
                response = self.server._GET(self.name)

    def compact_design(self, designname):
        "Compacts the view indexes associated with the named design document."
        self.server._POST(self.name, "_compact", designname,
                          headers={"Content-Type": JSON_MIME})

    def view_cleanup(self):
        """Removes unnecessary view index files due to changed views in
        design documents of the database.
        """
        self.server._POST(self.name, "_view_cleanup")

    def get(self, id, default=None, rev=None, revs_info=False, conflicts=False):
        """Returns the document with the given identifier,
        or the `default` value if not found.

        - `rev`: Retrieves document of specified revision, if specified.
        - `revs_info`: Whether to include detailed information for all known
          document revisions.
        - `conflicts`: Whether to include information about conflicts in
          the document in the `_conflicts` attribute.
        """
        params = {}
        if rev is not None:
            params["rev"] = rev
        if revs_info:
            params["revs_info"] = _jsons(True)
        if conflicts:
            params['conflicts'] = _jsons(True)
        response = self.server._GET(self.name, id,
                                    errors={404: None},
                                    params=params)
        if response.status_code == 404:
            return default
        return response.json()

    def get_bulk(self, ids):
        """Gets several documents in one operation, given a list of
        document identifiers, each of which is a string (the document `_id`),
        or a tuple of the document `_id` and `_rev`.

        Returns a list of documents. If no document found for a specified
        `_id` or `(_id, rev)`, the value None is returned in that slot
        of the list.
        """
        docs = []
        for id in ids:
            if isinstance(id, (tuple, list)):
                docs.append({"id": id[0], "rev": id[1]})
            else:
                docs.append({"id": id})
        response = self.server._POST(self.name, "_bulk_get", json={"docs": docs})
        return [i["docs"][0].get("ok") for i in response.json()["results"]]

    def ids(self):
        "Returns an iterator over all document identifiers."
        return _DatabaseIterator(self, include_docs=False)

    def put(self, doc):
        """Inserts or updates the document.

        If the document is already in the database, the `_rev` item must
        be present in the document, and it will be updated.

        If the document does not contain an item `_id`, it is added
        having a UUID4 hex value. The `_rev` item is also added.
        """
        if "_id" not in doc:
            doc["_id"] = uuid.uuid4().hex
        response = self.server._PUT(self.name, doc["_id"], json=doc)
        doc["_rev"] = response.json()["rev"]

    def update(self, docs):
        """Performs a bulk update or insertion of the given documents using a
        single HTTP request.

        Returns an iterable (list) over the resulting documents.

        `docs` is a sequence of dictionaries or `Document` objects, or
        objects providing an `items()` method that can be used to convert
        them to a dictionary.

        The return value of this method is a list containing a tuple for every
        element in the `docs` sequence. Each tuple is of the form
        `(success, docid, rev_or_exc)`, where `success` is a boolean
        indicating whether the update succeeded, `docid` is the ID of the
        document, and `rev_or_exc` is either the new document revision, or
        an exception instance (e.g. `ResourceConflict`) if the update failed.

        If an object in the docs list is not a dictionary, this method
        looks for an `items()` method that can be used to convert the object
        to a dictionary.
        """
        documents = []
        for doc in docs:
            if isinstance(doc, dict):
                documents.append(doc)
            elif hasattr(doc, "items"):
                documents.append(dict(doc.items()))
            else:
                raise TypeError("expected dict, got %s" % type(doc))

        response = self.server._POST(self.name + "/_bulk_docs",
                                 data=json.dumps({"docs": documents}),
                                 headers={"Content-Type": JSON_MIME})

        data = response.json()
        results = []
        for result in data:
            if "error" in result:
                results.append((False, result["id"], result["error"], result["reason"]))
            else:
                results.append((True, result["id"], result["rev"]))
        return results

    def delete(self, doc):
        "Deletes the document, which must contain the _id and _rev items."
        if "_id" not in doc:
            raise NotFoundError("missing '_id' item in the document")
        if "_rev" not in doc:
            raise RevisionError("missing '_rev' item in the document")
        response = self.server._DELETE(self.name, doc["_id"],
                                       headers={"If-Match": doc["_rev"]})

    def purge(self, docs):
        """Performs purging (complete removal) of the given list of documents.

        Uses a single HTTP request to purge all given documents. Purged
        documents do not leave any meta-data in the storage and are not
        replicated.
        """
        content = {}
        for doc in docs:
            if isinstance(doc, dict):
                content[doc["_id"]] = [doc["_rev"]]
            elif hasattr(doc, "items"):
                doc = dict(doc.items())
                content[doc["_id"]] = [doc["_rev"]]
            else:
                raise TypeError("expected dict, got %s" % type(doc))
        response = self.server._POST(self.name + "/_purge",
                                     data=json.dumps(content),
                                     headers={"Content-Type": JSON_MIME})
        return response.json()

    def get_designs(self):
        """Returns the design documents for the database.

        **NOTE:** CouchDB version >= 2.2.
        """
        assert self.server.version >= "2.2"
        response = self.server._GET(self.name, "_design_docs")
        return response.json()

    def get_design(self, designname):
        "Gets the named design document."
        response = self.server._GET(self.name, "_design", designname)
        return response.json()

    def put_design(self, designname, doc, rebuild=True):
        """Inserts or updates the design document under the given name.

        If the existing design document is identical, no action is taken and
        False is returned, else the document is updated and True is returned.

        If `rebuild` is True, force view indexes to be rebuilt after update
        by accessing the view. This may take some time.

        Example of doc:
        ```
          {"views":
            {"name":
              {"map": "function (doc) {emit(doc.name, null);}"},
             "name_sum":
              {"map": "function (doc) {emit(doc.name, 1);}",
               "reduce": "_sum"},
             "name_count":
              {"map": "function (doc) {emit(doc.name, null);}",
               "reduce": "_count"}
          }}
        ```

        More info: http://docs.couchdb.org/en/latest/api/ddoc/common.html
        """
        response = self.server._GET(self.name, "_design", designname,
                                    errors={404: None})
        if response.status_code == 200:
            current_doc = response.json()
            doc["_id"] = current_doc["_id"]
            doc["_rev"] = current_doc["_rev"]
            if doc == current_doc:
                return False
        response = self.server._PUT(self.name, "_design", designname, json=doc)
        if rebuild:
            for view in doc.get("views", {}):
                self.view(designname, view, limit=1)
        return True

    def view(self, designname, viewname,
             key=None, keys=None, startkey=None, endkey=None,
             skip=None, limit=None, sorted=True, descending=False,
             group=False, group_level=None, reduce=None, include_docs=False,
             update=None):
        """Query a view index to obtain data and/or documents.

        Keyword arguments:

        - `key`: Return only rows that match the specified key.
        - `keys`: Return only rows where they key matches one of
          those specified as a list.
        - `startkey`: Return rows starting with the specified key.
        - `endkey`: Stop returning rows when the specified key is reached.
        - `skip`: Skip the number of rows before starting to return rows.
        - `limit`: Limit the number of rows returned.
        - `sorted=True`: Sort the rows by the key of each returned row.
          The items `total_rows` and `offset` are not available if 
          set to `False`.
        - `descending=False`: Return the rows in descending order.
        - `group=False`: Group the results using the reduce function of
          the design document to a group or a single row. If set to `True`
          implies `reduce` to `True` and defaults the `group_level` 
          to the maximum.
        - `group_level`: Specify the group level to use. Implies `group`
          is `True`.
        - `reduce`: If set to `False` then do not use the reduce function 
          if there is one defined in the design document. Default is to
          use the reduce function if defined.
        - `include_docs=False`: If `True`, include the document for each row.
          This will force `reduce` to `False`.
        - `update="true"`: Whether ir not the view should be updated prior to
          returning the result. Supported value are `"true"`, `"false"`
          and `"lazy"`.

        Returns a ViewResult instance, containing the following attributes:

        - `rows`: the list of Row instances.
        - `offset`: the offset used for the set of rows.
        - `total_rows`: the total number of rows selected.

        A Row object contains the following attributes:

        - `id`: the identifier of the document, if any.
        - `key`: the key for the index row.
        - `value`: the value for the index row.
        - `doc`: the document, if any.
        """
        params = {}
        if startkey is not None:
            params["startkey"] = _jsons(startkey)
        if key is not None:
            params["key"] = _jsons(key)
        if keys is not None:
            params["keys"] = _jsons(keys)
        if endkey is not None:
            params["endkey"] = _jsons(endkey)
        if skip is not None:
            params["skip"] = _jsons(skip)
        if limit is not None:
            params["limit"] = _jsons(limit)
        if not sorted:
            params["sorted"] = _jsons(False)
        if descending:
            params["descending"] = _jsons(True)
        if group:
            params["group"] = _jsons(True)
        if group_level is not None:
            params["group_level"] = _jsons(group_level)
        if reduce is not None:
            params["reduce"] = _jsons(bool(reduce))
        if include_docs:
            params["include_docs"] = _jsons(True)
            params["reduce"] = _jsons(False)
        if update is not None:
            assert update in ["true","false","lazy"]
            params["update"] = update
        response = self.server._GET(self.name, "_design", designname,
                                    "_view", viewname, params=params)
        data = response.json()
        return ViewResult([Row(r.get("id"), r.get("key"), r.get("value"),
                               r.get("doc")) for r in data.get("rows", [])],
                          data.get("offset"),
                          data.get("total_rows"))

    def get_indexes(self):
        """Returns a list of all indexes in the database.

        CouchDB version >= 2.0
        """
        assert self.server.version >= "2.0"
        response = self.server._GET(self.name, "_index")
        return response.json()

    def put_index(self, fields, ddoc=None, name=None, selector=None):
        """Stores a Mango index specification.

        - `fields` is a list of fields to index.
        - `ddoc` is the design document name. Generated if none given.
        - `name` is the name of the index. Generated if none given.
        - `selector` is a partial filter selector, which may be omitted.

        Returns a dictionary with items `id` (design document identifier; sic!),
        `name` (index name) and `result` (`created` or `exists`).

        CouchDB version >= 2.0
        """
        assert self.server.version >= "2.0"
        doc = {"index": {"fields": fields}}
        if ddoc is not None:
            doc["ddoc"] = ddoc
        if name is not None:
            doc["name"] = name
        if selector is not None:
            doc["index"]["partial_filter_selector"] = selector
        response = self.server._POST(self.name, "_index", json=doc)
        return response.json()

    def delete_index(designname, name):
        """Deletes the named index in the design document of the given name.

        CouchDB version >= 2.0
        """
        assert self.server.version >= "2.0"
        self.server._DELETE(self.name, "_index", designname, "json", name)

    def find(self, selector, limit=None, skip=None, sort=None, fields=None,
             use_index=None, bookmark=None, update=True, conflicts=False):
        """Selects documents according to the Mango index `selector`.

        - `selector`: The Mango index. For more information on selector syntax,
          see https://docs.couchdb.org/en/latest/api/database/find.html#find-selectors
        - `limit`: Maximum number of results returned.
        - `skip`: Skip the given number of results.
        - `sort`: A list of dictionaries specifying the order of the results,
          where the field name is the key and the direction is the value;
          either `"asc"` or `"desc"`.
        - `fields`: List specifying which fields of each result document should
          be returned. If omitted, return the entire document.
        - `use_index`: String or list of strings specifying the index(es)
          to use.
        - `bookmark`: A string that marks the end the previous set of results.
          If given, the next set of results will be returned.
        - `update`: Whether to update the index prior to returning the result.
        - `conflicts`: Whether to include conflicted documents.

        Returns a dictionary with items `docs`, `warning`, `execution_stats`
        and `bookmark`.

        CouchDB version >= 2.0
        """
        assert self.server.version >= "2.0"
        doc = {"selector": selector,
               "update": bool(update),
               "conflicts": bool(conflicts)}
        if limit is not None:
            doc["limit"] = limit
        if skip is not None:
            doc["skip"] = skip
        if sort is not None:
            doc["sort"] = sort
        if fields is not None:
            doc["fields"] = fields
        if use_index is not None:
            doc["use_index"] = use_index
        if bookmark is not None:
            doc["bookmark"] = bookmark
        response = self.server._POST(self.name, "_find", json=doc)
        return response.json()

    def explain(self, selector, limit=None, skip=None, 
                sort=None, fields=None, bookmark=None):
        """Return info on which index is being used by the query.

        - `selector`: The Mango index. For more information on selector syntax,
          see https://docs.couchdb.org/en/latest/api/database/find.html#find-selectors
        - `limit`: Maximum number of results returned.
        - `skip`: Skip the given number of results.
        - `sort`: A list of dictionaries specifying the order of the results,
          where the field name is the key and the direction is the value;
          either `"asc"` or `"desc"`.
        - `fields`: List specifying which fields of each result document should
          be returned. If omitted, return the entire document.
        - `bookmark`: A string that marks the end the previous set of results.
          If given, the next set of results will be returned.

        CouchDB version >= 2.0
        """
        assert self.server.version >= "2.0"
        doc = {"selector": selector, "update": bool(update)}
        if limit is not None:
            doc["limit"] = limit
        if skip is not None:
            doc["skip"] = skip
        if sort is not None:
            doc["sort"] = sort
        if fields is not None:
            doc["fields"] = fields
        if bookmark is not None:
            doc["bookmark"] = bookmark
        response = self.server._POST(self.name, "_explain", json=doc)
        return response.json()

    def get_attachment(self, doc, filename):
        "Returns a file-like object containing the content of the attachment."
        response = self.server._GET(self.name, doc["_id"], filename,
                                    params={"rev": doc["_rev"]})
        return io.BytesIO(response.content)

    def put_attachment(self, doc, content, filename=None, content_type=None):
        """Adds or updates the given file as an attachment to 
        the given document in the database.

        - `content` is a string or a file-like object.
        - If `filename` is not provided, then an attempt is made to get it from
          the `content` object. If this fails, `ValueError` is raised.
        - If `content_type` is not provided, then an attempt to guess it from
          the filename extension is made. If that does not work, it is
          set to `"application/octet-stream"`

        Returns the new revision of the document, in addition to updating
        the `_rev` field in the document.
        """
        if filename is None:
            try:
                filename = content.name
            except AttributeError:
                raise ValueError("could not figure out filename")
        if not content_type:
            (content_type, enc) = mimetypes.guess_type(filename, strict=False)
            if not content_type: content_type = BIN_MIME
        response = self.server._PUT(self.name, doc["_id"], filename,
                                    data=content,
                                    headers={"Content-Type": content_type,
                                             "If-Match": doc["_rev"]})
        doc["_rev"] = response.json()["rev"]
        # Return the new `_rev` for backwards compatibility reasons.
        return doc["_rev"]

    def delete_attachment(self, doc, filename):
        """Deletes the attachment.

        Returns the new revision of the document, in addition to updating
        the `_rev` field in the document.
        """
        response = self.server._DELETE(self.name, doc["_id"], filename,
                                       headers={"If-Match": doc["_rev"]})
        doc["_rev"] = response.json()["rev"]
        # Return the new `_rev` for backwards compatibility reasons.
        return doc["_rev"]

    def changes(self, doc_ids=None, conflicts=None, descending=None,
                feed="normal", filter=None, heartbeat=None, include_docs=None,
                attachments=None, att_encoding_info=None, last_event_id=None,
                limit=None, since=None, style=None, timeout=None, view=None,
                seq_interval=None):
        """Returns a sorted list of changes made to documents in the database,
        in time order of application.

        Refer to the CouchDB documentation
        https://docs.couchdb.org/en/stable/api/database/changes.html

        NOTE: This implementation has not been adequately tested.
        In particular, the behaviour with a 'feed' value other than
        the default 'normal' is unknown.
        """
        params = {}
        data = None
        if doc_ids is not None:
            params["filter"] = "_doc_ids"
            data = dict(doc_ids=doc_ids)
        if conflicts is not None:
            params["conflicts"] = _jsons(conflicts)
        if feed is not None:
            params["feed"] = feed
        if filter is not None:
            params["filter"] = filter
        if heartbeat is not None:
            params["heartbeat"] = _jsons(heartbeat)
        if include_docs is not None:
            params["include_docs"] = _jsons(include_docs)
        if attachments is not None:
            params["attachments"] = _jsons(attachments)
        if att_encoding_info is not None:
            params["att_encoding_info"] = _jsons(att_encoding_info)
        if last_event_id is not None:
            params["last-event-id"] = _jsons(last_event_id)
        if limit is not None:
            params["limit"] = _jsons(limit)
        if since is not None:
            params["since"] = _jsons(since)
        if style is not None:
            params["style"] = style
        if timeout is not None:
            params["timeout"] = _jsons(timeout)
        if view is not None:
            params["view"] = view
        if seq_interval is not None:
            params["seq_interval"] = _jsons(seq_interval)
        response = self.server._POST(self.name, "_changes",
                                     params=params, json=data,
                                     headers={"Content-Type": JSON_MIME})
        return response.json()
            
    def dump(self, filepath, callback=None):
        """Dumps the entire database to a `tar` file.

        Returns a tuple `(ndocs, nfiles)` giving the number of documents
        and attached files written out.

        If defined, the function `callback(ndocs, nfiles)` is called
        every 100 documents.

        If the filepath ends with `.gz`, then the tar file is gzip compressed.
        The `_rev` item of each document is written out.
        """
        ndocs = 0
        nfiles = 0
        if filepath.endswith(".gz"):
            mode = "w:gz"
        else:
            mode = "w"
        with tarfile.open(filepath, mode=mode) as outfile:
            for doc in self:
                info = tarfile.TarInfo(doc["_id"])
                data = _jsons(doc).encode("utf-8")
                info.size = len(data)
                outfile.addfile(info, io.BytesIO(data))
                ndocs += 1
                # Attachments must follow their document.
                for attname in doc.get("_attachments", dict()):
                    info = tarfile.TarInfo(u"{}_att/{}".format(doc["_id"],
                                                               attname))
                    attfile = self.get_attachment(doc, attname)
                    if attfile is None:
                        attdata = ""
                    else:
                        attdata = attfile.read()
                        attfile.close()
                    info.size = len(attdata)
                    outfile.addfile(info, io.BytesIO(attdata))
                    nfiles += 1
                if ndocs % 100 == 0 and callback:
                    callback(ndocs, nfiles)
        return (ndocs, nfiles)

    def undump(self, filepath, callback=None):
        """Loads the `tar` file given by the path. It must have been
        produced by `db.dump`.

        Returns a tuple `(ndocs, nfiles)` giving the number of documents
        and attached files read from the file.

        If defined, the function `callback(ndocs, nfiles)` is called
        every 100 documents.

        NOTE: The documents are just added to the database, ignoring any
        `_rev` items. This means that no document with the same identifier
        may exist in the database.
        """
        ndocs = 0
        nfiles = 0
        atts = dict()
        with tarfile.open(filepath, mode="r") as infile:
            for item in infile:
                itemfile = infile.extractfile(item)
                itemdata = itemfile.read()
                itemfile.close()
                if item.name in atts:
                    # An attachment follows its document.
                    self.put_attachment(doc, itemdata, **atts.pop(item.name))
                    nfiles += 1
                else:
                    doc = json.loads(itemdata.decode("utf-8"))
                    doc.pop("_rev", None)
                    atts = doc.pop("_attachments", dict())
                    self.put(doc)
                    ndocs += 1
                    for attname, attinfo in list(atts.items()):
                        key = u"{}_att/{}".format(doc["_id"], attname)
                        atts[key] = dict(filename=attname,
                                         content_type=attinfo["content_type"])
                if ndocs % 100 == 0 and callback:
                    callback(ndocs, nfiles)
        return (ndocs, nfiles)


class _DatabaseIterator(object):
    "Iterator over all documents, or all document identifiers, in a database."

    def __init__(self, db, limit=CHUNK_SIZE, include_docs=True):
        self.db = db
        self.params = {"include_docs": bool(include_docs),
                       "limit": int(limit),
                       "skip": 0}
        self.chunk = []

    def __iter__(self):
        return self

    def __next__(self):
        return self.next()

    def next(self):
        try:
            return self.chunk.pop()
        except IndexError:
            response = self.db.server._GET(self.db.name,
                                           "_all_docs",
                                           params=self.params)
            data = response.json()
            rows = data["rows"]
            if len(rows) == 0:
                raise StopIteration
            if self.params["include_docs"]:
                self.chunk = [r["doc"] for r in rows]
            else:
                self.chunk = [r["id"] for r in rows]
            self.chunk.reverse()
            self.params["skip"] = data["offset"] + len(self.chunk)
            return self.chunk.pop()


class ViewResult(object):
    """Result of view query; contains rows, offset, total_rows.
    Instances of this class are not supposed to be created by client software.
    """

    def __init__(self, rows, offset, total_rows):
        self.rows = rows
        self.offset = offset
        self.total_rows = total_rows

    def __len__(self):
        return len(self.rows)

    def __getitem__(self, i):
        return self.rows[i]

    def __iter__(self):
        return iter(self.rows)

    def json(self):
        "Return data in a JSON-like representation."
        result = dict()
        result["total_rows"] = self.total_rows
        result["offset"] = self.offset
        return result


Row = collections.namedtuple("Row", ["id", "key", "value", "doc"])


class CouchDB2Exception(Exception):
    "Base CouchDB2 exception."


class NotFoundError(CouchDB2Exception):
    "No such entity exists."


class BadRequestError(CouchDB2Exception):
    "Invalid request; bad name, body or headers."


class CreationError(CouchDB2Exception):
    "Could not create the entity; it exists already."


class RevisionError(CouchDB2Exception):
    "Wrong or missing '_rev' item in the document to put."


class AuthorizationError(CouchDB2Exception):
    "Current user not authorized to perform the operation."


class ContentTypeError(CouchDB2Exception):
    "Bad 'Content-Type' value in the request."


class ServerError(CouchDB2Exception):
    "Internal CouchDB server error."


_ERRORS = {200: None,
           201: None,
           202: None,
           304: None,
           400: BadRequestError,
           401: AuthorizationError,
           403: AuthorizationError,
           404: NotFoundError,
           409: RevisionError,
           412: CreationError,
           415: ContentTypeError,
           500: ServerError}


def _jsons(data, indent=None):
    "Convert data into JSON string."
    return json.dumps(data, ensure_ascii=False, indent=indent)


def _get_parser():
    "Get the parser for the command line tool."
    p = argparse.ArgumentParser(prog="couchdb2", usage="%(prog)s [options]",
                                description="CouchDB v2.x command line tool,"
                                            " leveraging Python module CouchDB2.")
    p.add_argument("--settings", metavar="FILEPATH",
                   help="settings file in JSON format")
    p.add_argument("-S", "--server",
                   help="CouchDB server URL, including port number")
    p.add_argument("-d", "--database", help="database to operate on")
    p.add_argument("-u", "--username", help="CouchDB user account name")
    x01 = p.add_mutually_exclusive_group()
    x01.add_argument("-p", "--password", help="CouchDB user account password")
    x01.add_argument("-q", "--password_question", action="store_true",
                     help="ask for the password by interactive input")
    p.add_argument("--ca_file", metavar="FILEORDIRPATH",
                   help="file or directory containing CAs")
    p.add_argument("-o", "--output", metavar="FILEPATH",
                   help="write output to the given file (JSON format)")
    p.add_argument("--indent", type=int, metavar="INT",
                   help="indentation level for JSON format output file")
    p.add_argument("-y", "--yes", action="store_true",
                   help="do not ask for confirmation (delete, destroy)")
    x02 = p.add_mutually_exclusive_group()
    x02.add_argument("-v", "--verbose", action="store_true",
                     help="print more information")
    x02.add_argument("-s", "--silent", action="store_true",
                     help="print no information")

    g0 = p.add_argument_group("server operations")
    g0.add_argument("-V", "--version", action="store_true",
                    help="output CouchDB server version")
    g0.add_argument("--list", action="store_true",
                    help="output a list of the databases on the server")

    g1 = p.add_argument_group("database operations")
    x11 = g1.add_mutually_exclusive_group()
    x11.add_argument("--create", action="store_true",
                     help="create the database")
    x11.add_argument("--destroy", action="store_true",
                     help="delete the database and all its contents")
    g1.add_argument("--compact", action="store_true",
                    help="compact the database; may take some time")
    g1.add_argument("--compact_design", metavar="DDOC",
                    help="compact the view indexes for the named design doc")
    g1.add_argument("--view_cleanup", action="store_true",
                    help="remove view index files no longer required")
    g1.add_argument("--info", action="store_true",
                    help="output information about the database")
    x12 = g1.add_mutually_exclusive_group()
    x12.add_argument("--security", action="store_true",
                     help="output security information for the database")
    x12.add_argument("--set_security", metavar="FILEPATH",
                     help="set security information for the database"
                          " from the JSON file")
    x13 = g1.add_mutually_exclusive_group()
    x13.add_argument("--list_designs", action="store_true",
                     help="list design documents for the database")
    x13.add_argument("--design", metavar="DDOC",
                     help="output the named design document")
    x13.add_argument("--put_design", nargs=2, metavar=("DDOC", "FILEPATH"),
                     help="store the named design document from the file")
    x13.add_argument("--delete_design", metavar="DDOC",
                     help="delete the named design document")
    x14 = g1.add_mutually_exclusive_group()
    x14.add_argument("--dump", metavar="FILEPATH",
                     help="create a dump file of the database")
    x14.add_argument("--undump", metavar="FILEPATH",
                     help="load a dump file into the database")

    g2 = p.add_argument_group("document operations")
    x2 = g2.add_mutually_exclusive_group()
    x2.add_argument("-G", "--get", metavar="DOCID",
                    help="output the document with the given identifier")
    x2.add_argument("-P", "--put", metavar="FILEPATH",
                    help="store the document; arg is literal doc or filepath")
    x2.add_argument("--delete", metavar="DOCID",
                    help="delete the document with the given identifier")

    g3 = p.add_argument_group("attachments to document")
    x3 = g3.add_mutually_exclusive_group()
    x3.add_argument("--attach", nargs=2, metavar=("DOCID", "FILEPATH"),
                    help="attach the specified file to the given document")
    x3.add_argument("--detach", nargs=2, metavar=("DOCID", "FILENAME"),
                    help="remove the attached file from the given document")
    x3.add_argument("--get_attach", nargs=2, metavar=("DOCID", "FILENAME"),
                    help="get the attached file from the given document;"
                         " write to same filepath or that given by '-o'")

    g4 = p.add_argument_group("query a design view, returning rows")
    g4.add_argument("--view", metavar="SPEC",
                    help="design view '{design}/{view}' to query")
    x41 = g4.add_mutually_exclusive_group()
    x41.add_argument("--key", metavar="KEY",
                     help="key value selecting view rows")
    x41.add_argument("--startkey", metavar="KEY",
                     help="start key value selecting range of view rows")
    g4.add_argument("--endkey", metavar="KEY",
                    help="end key value selecting range of view rows")
    g4.add_argument("--startkey_docid", metavar="DOCID",
                    help="return rows starting with the specified document")
    g4.add_argument("--endkey_docid", metavar="DOCID",
                    help="stop returning rows when specified document reached")
    g4.add_argument("--group", action="store_true",
                    help="group the results using the 'reduce' function")
    g4.add_argument("--group_level", type=int, metavar="INT",
                    help="specify the group level to use")
    g4.add_argument("--noreduce", action="store_true",
                    help="do not use the 'reduce' function of the view")
    g4.add_argument("--limit", type=int, metavar="INT",
                    help="limit the number of returned rows")
    g4.add_argument("--skip", type=int, metavar="INT",
                    help="skip this number of rows before returning result")
    g4.add_argument("--descending", action="store_true",
                    help="sort rows in descending order (swap start/end keys!)")
    g4.add_argument("--include_docs", action="store_true",
                    help="include documents in result")
    return p


def _get_settings(pargs):
    """Get the settings lookup for the command line tool.
    1) Initialize with DEFAULT_SETTINGS
    2) Update with values in JSON file in DEFAULT_SETTINGS_FILEPATHS, if any.
    3) Update with values in the explicitly given settings file, if any.
    4) Update from environment variables.
    5) Update from command line arguments.
    """
    settings = DEFAULT_SETTINGS.copy()
    filepaths = DEFAULT_SETTINGS_FILEPATHS[:]
    if pargs.settings:
        filepaths.append(pargs.settings)
    for filepath in filepaths:
        try:
            settings = read_settings(filepath, settings=settings)
            _verbose(pargs, "Settings read from file", filepath)
        except IOError:
            _verbose(pargs, "Warning: no settings file", filepath)
        except (ValueError, TypeError):
            sys.exit("Error: bad settings file", filepath)
    for key in ["SERVER", "DATABASE", "USERNAME", "PASSWORD"]:
        try:
            settings[key] = os.environ[key]
        except KeyError:
            pass
    if pargs.server:
        settings["SERVER"] = pargs.server
    if pargs.database:
        settings["DATABASE"] = pargs.database
    if pargs.username:
        settings["USERNAME"] = pargs.username
    if pargs.password:
        settings["PASSWORD"] = pargs.password
    if pargs.verbose:
        s = dict()
        for key in ["SERVER", "DATABASE", "USERNAME"]:
            s[key] = settings[key]
        if settings["PASSWORD"] is None:
            s["PASSWORD"] = None
        else:
            s["PASSWORD"] = "***"
        print("Settings:", _jsons(s, indent=2))
    return settings


DEFAULT_SETTINGS = {"SERVER": "http://localhost:5984",
                    "DATABASE": None,
                    "USERNAME": None,
                    "PASSWORD": None}

DEFAULT_SETTINGS_FILEPATHS = ["~/.couchdb2", "settings.json"]


def read_settings(filepath, settings=None):
    """Read the settings lookup from a JSON format file.
    If `settings` is given, then return an updated copy of it,
    else copy the default settings, update, and return.
    """
    if settings:
        result = settings.copy()
    else:
        result = DEFAULT_SETTINGS.copy()
    with open(os.path.expanduser(filepath), "rb") as infile:
        data = json.load(infile)
        for key in DEFAULT_SETTINGS:
            for prefix in ["", "COUCHDB_", "COUCHDB2_"]:
                try:
                    result[key] = data[prefix + key]
                except KeyError:
                    pass
    return result


def _get_database(server, settings):
    "Get the database defined in the settings,"
    if not settings["DATABASE"]:
        sys.exit("Error: no database defined")
    return server[settings["DATABASE"]]

def _message(pargs, *args):
    "Unless flag '--silent' was used, print the arguments."
    if pargs.silent: return
    print(*args)

def _verbose(pargs, *args):
    "If flag '--verbose' was used, then print the arguments."
    if not pargs.verbose: return
    print(*args)

def _json_output(pargs, data, else_print=False):
    """If `--output` was used, write the data in JSON format to the file.
    The indentation level is set by `--indent`.
    If the filepath ends in `.gz`. then a gzipped file is produced.

    If `--output` was not used and `else_print` is True,
    then use `print()` for indented JSON output.

    Return True if `--output` was used, else False.
    """
    if pargs.output:
        if pargs.output.endswith(".gz"):
            with gzip.open(pargs.output, "w") as outfile:
                js = json.dumps(data, ensure_ascii=False, indent=pargs.indent)
                outfile.write(js.encode("utf-8"))
        else:
            with io.open(pargs.output, "w", encoding="utf-8") as outfile:
                js = json.dumps(data, ensure_ascii=False, indent=pargs.indent)
                outfile.write(unicode(js))
        _verbose(pargs, "Wrote JSON to file", pargs.output)
    elif else_print:
        print(_jsons(data, indent=2))
    return bool(pargs.output)

def json_input(filepath):
    "Read the JSON document file."
    try:
        with open(filepath, "r") as infile:
            return json.load(infile, )
    except (IOError, ValueError, TypeError) as error:
        sys.exit(f"Error: {error}")

def _print_dot(*args):
    "Print a dot without a newline and flush immediately."
    print(".", sep="", end="")
    sys.stdout.flush()

def _execute(pargs, settings):
    "Execution of the CouchDB2 command line tool."
    server = Server(href=settings["SERVER"],
                    username=settings["USERNAME"],
                    password=settings["PASSWORD"],
                    ca_file=pargs.ca_file)
    if pargs.verbose and server.user_context:
        print("User context:", _jsons(server.user_context, indent=2))
    if pargs.version:
        if not _json_output(pargs, server.version):
            print(server.version)
    if pargs.list:
        dbs = list(server)
        if not _json_output(pargs, [str(db) for db in dbs]):
            for db in dbs:
                print(db)

    if pargs.create:
        db = server.create(settings["DATABASE"])
        _message(pargs, "Created database", db)
    elif pargs.destroy:
        db = _get_database(server, settings)
        if not pargs.yes:
            answer = input(f"Really destroy database '{db}' [n] ? ")
            if answer and answer.lower()[0] in ("y", "t"):
                pargs.yes = True
        if pargs.yes:
            db.destroy()
            _message(pargs, f"Destroyed database '{db}'.")

    if pargs.compact:
        db = _get_database(server, settings)
        if pargs.silent:
            db.compact(finish=True)
        else:
            print(f"Compacting '{db}'.", sep="", end="")
            sys.stdout.flush()
            db.compact(finish=True, callback=_print_dot)
            print()
    if pargs.compact_design:
        _get_database(server, settings).compact_design(pargs.compact_design)
    if pargs.view_cleanup:
        _get_database(server, settings).view_cleanup()

    if pargs.info:
        db = _get_database(server, settings)
        _json_output(pargs, db.get_info(), else_print=True)

    if pargs.security:
        db = _get_database(server, settings)
        _json_output(pargs, db.get_security(), else_print=True)
    elif pargs.set_security:
        db = _get_database(server, settings)
        db.set_security(json_input(pargs.set_security))

    if pargs.list_designs:
        data = _get_database(server, settings).get_designs()
        if not _json_output(pargs, data):
            for row in data["rows"]:
                print(row["id"][len("_design/"):])
    elif pargs.design:
        db = _get_database(server, settings)
        _json_output(pargs, db.get_design(pargs.get_design), else_print=True)
    elif pargs.put_design:
        doc = json_input(pargs.put_design[1])
        _get_database(server, settings).put_design(pargs.put_design[0], doc)
        _message(pargs, "Stored design", pargs.put_design[0])
    elif pargs.delete_design:
        db = _get_database(server, settings)
        doc = db.get_design(pargs.delete_design)
        db.delete(doc)
        _message(pargs, "Deleted design", pargs.delete_design)

    if pargs.get:
        doc = _get_database(server, settings)[pargs.get.decode("utf-8")]
        _json_output(pargs, doc, else_print=True)
    elif pargs.put:
        try:  # Attempt to interpret arg as explicit doc
            doc = json.loads(pargs.put)
        except (ValueError, TypeError):  # Arg is filepath to doc
            doc = json_input(pargs.put)
        _get_database(server, settings).put(doc)
        _message(pargs, "Stored doc", doc["_id"])
    elif pargs.delete:
        db = _get_database(server, settings)
        doc = db[pargs.delete]
        db.delete(doc)
        _message(pargs, "Deleted doc", doc["_id"])

    if pargs.attach:
        db = _get_database(server, settings)
        doc = db[pargs.attach[0]]
        with open(pargs.attach[1], "rb") as infile:
            # Non-trivial decision: concluded that it is the basename of
            # the file that is the best identifier for the attachment,
            # not the entire filepath.
            db.put_attachment(doc, infile,
                              filename=os.path.basename(pargs.attach[1]))
        _message(pargs, "Attached file '{1}' to doc '{0}'".format(*pargs.attach))
    elif pargs.detach:
        db = _get_database(server, settings)
        doc = db[pargs.detach[0]]
        db.delete_attachment(doc, pargs.detach[1])
        _message(pargs,
                "Detached file '{1}' from doc '{0}'".format(*pargs.detach))
    elif pargs.get_attach:
        db = _get_database(server, settings)
        doc = db[pargs.get_attach[0]]
        filepath = pargs.output or pargs.get_attach[1]
        with open(filepath, "wb") as outfile:
            outfile.write(db.get_attachment(doc, pargs.get_attach[1]).read())
        _message(pargs,
                 "Wrote file '{0}' from doc '{1}' attachment '{2}'".format(
                     filepath, *pargs.get_attach))

    if pargs.view:
        try:
            design, view = pargs.view.split("/")
        except ValueError:
            sys.exit("Error: invalid view specification")
        kwargs = {}
        for key in ("key", "startkey", "endkey", "startkey_docid",
                    "endkey_docid", "group", "group_level", "limit", "skip",
                    "descending", "include_docs"):
            value = getattr(pargs, key)
            if value is not None:
                kwargs[key] = value
        if pargs.noreduce:
            kwargs["reduce"] = False
        result = _get_database(server, settings).view(design, view, **kwargs)
        _json_output(pargs, result.json(), else_print=True)

    if pargs.dump:
        db = _get_database(server, settings)
        if pargs.silent:
            db.dump(pargs.dump)
        else:
            print(f"Dumping '{db}'.", sep="", end="")
            sys.stdout.flush()
            ndocs, nfiles = db.dump(pargs.dump, callback=_print_dot)
            print()
            print(f"Dumped {ndocs} documents, {nfiles} files.")
    elif pargs.undump:
        db = _get_database(server, settings)
        if len(db) != 0:
            sys.exit(f"Error: Database '{db}' is not empty")
        if pargs.silent:
            db.undump(pargs.undump)
        else:
            print(f"Undumping '{db}'.", sep="", end="")
            sys.stdout.flush()
            ndocs, nfiles = db.undump(pargs.undump, callback=_print_dot)
            print()
            print(f"Undumped {ndocs} documents, {nfiles} files.")


def main():
    "Entry point for the CouchDB2 command line tool."
    try:
        parser = _get_parser()
        pargs = parser.parse_args()
        if len(sys.argv) == 1:
            parser.print_usage()
        settings = _get_settings(pargs)
        if pargs.password_question:
            settings["PASSWORD"] = getpass.getpass("password > ")
        _execute(pargs, settings)
    except CouchDB2Exception as error:
        sys.exit(f"Error: {error}")


if __name__ == "__main__":
    main()
