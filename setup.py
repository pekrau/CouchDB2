from setuptools import setup

URL = "https://github.com/pekrau/CouchDB2"
AUTHOR = "Per Kraulis"
AUTHOR_EMAIL = "per.kraulis@gmail.com"

LICENSE = "MIT"
LICENSE_CLASSIFIER = "License :: OSI Approved :: MIT License"

import couchdb2

VERSION = couchdb2.__version__
DESCRIPTION = couchdb2.__doc__[0]

with open("README.md", "r") as infile:
    LONG_DESCRIPTION = infile.read()

setup(name="CouchDB2",
      version=VERSION,
      description=DESCRIPTION,
      long_description=LONG_DESCRIPTION,
      long_description_content_type="text/markdown",
      url=URL,
      author=AUTHOR,
      author_email=AUTHOR_EMAIL,
      license="MIT",
      py_modules=["couchdb2"],
      install_requires=[
          "requests>=2",
      ],
      entry_points={
          "console_scripts": ["couchdb2=couchdb2:main"]
      },
      classifiers=[
          LICENSE_CLASSIFIER,
          "Intended Audience :: Developers",
          "Natural Language :: English",
          "Development Status :: 3 - Alpha",
          "Programming Language :: Python :: 3",
          "Programming Language :: Python :: 3.6",
          "Operating System :: OS Independent",
          "Topic :: Database :: Front-Ends",
      ],
)
