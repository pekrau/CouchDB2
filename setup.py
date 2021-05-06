from setuptools import setup

import couchdb2


setup(name="CouchDB2",
      version=couchdb2.__version__,
      description=couchdb2.__doc__[0],
      long_description=open("README.md", "r").read(),
      long_description_content_type="text/markdown",
      url="https://github.com/pekrau/CouchDB2",
      author="Per Kraulis",
      author_email="per.kraulis@gmail.com",
      license="MIT",
      python_requires=">= 3.6",
      py_modules=["couchdb2"],
      install_requires=[
          "requests>=2",
      ],
      entry_points={
          "console_scripts": ["couchdb2=couchdb2:main"]
      },
      classifiers=[
          "License :: OSI Approved :: MIT License",
          "Intended Audience :: Developers",
          "Natural Language :: English",
          "Development Status :: 3 - Alpha",
          "Programming Language :: Python :: 3 :: Only",
          "Programming Language :: Python :: 3.6",
          "Operating System :: OS Independent",
          "Environment :: Console",
          "Topic :: Database :: Front-Ends",
          "Topic :: Software Development :: Libraries :: Python Modules"
      ],
)
