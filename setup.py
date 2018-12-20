from distutils.core import setup
setup(name='couchdb2',
      version='0.9.1',
      description='Slim Python interface module to CouchDB v2.x.',
      url='https://github.com/pekrau/CouchDB2',
      author='Per Kraulis',
      author_email='per.kraulis@scilifelab.se',
      license='MIT',
      py_modules=['couchdb2'],
      install_requires=[
          'requests',
      ],
)
