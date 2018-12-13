# -*- coding: utf-8 -*-
"""Python interface to CouchDB version 2 and higher. 
Relies on requests, http://docs.python-requests.org/en/master/
"""

from __future__ import print_function, unicode_literals

import collections
import json
import re
import uuid

import requests

__version__ = '0.0.1'

_NOTHING = object()
_RE_ARRAY_INDEX = re.compile('0|[1-9][0-9]*$')
_RE_NAME = re.compile('[a-zA-Z]\w*$')


class CouchDB2(Exception):
    "Base CouchDB2 exception class."
    pass


class Server(object):
    "Connection to the CouchDB server."

    def __init__(self, href=''):
        raise NotImplementedError


class Database(object):
    "Connection to a CouchDB database."
    pass


class Document(collections.OrderedDict):
    "A CouchDB document."
    pass
