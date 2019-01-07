from distutils.core import setup

with open('README.md', 'r') as infile:
    long_description = infile.read()

setup(name='CouchDB2',
      version='1.5.3',
      description='Slim Python interface module for CouchDB v2.x. Also a command line tool.',
      long_description=long_description,
      url='https://github.com/pekrau/CouchDB2',
      author='Per Kraulis',
      author_email='per.kraulis@scilifelab.se',
      license='MIT',
      py_modules=['couchdb2'],
      install_requires=[
          'requests>=2',
      ],
      classifiers=[
          'Intended Audience :: Developers',
          'Development-Status :: 4-Beta',
          'Programming Language :: Python :: 2.7',
          'Programming Language :: Python :. 3.6',
          'Topic :: Database :: Front-Ends',
      ],
)
