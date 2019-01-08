import setuptools

with open('README.md', 'r') as infile:
    long_description = infile.read()

setuptools.setup(name='CouchDB2',
      version='1.5.6',
      description='Slim Python interface module for CouchDB v2.x. Also a command line tool.',
      long_description=long_description,
      long_description_content_type='text/markdown',
      url='https://github.com/pekrau/CouchDB2',
      author='Per Kraulis',
      author_email='per.kraulis@scilifelab.se',
      license='MIT',
      py_modules=['couchdb2'],
      install_requires=[
          'requests>=2',
      ],
      entry_points={
          'console_scripts': ['couchdb2=couchdb2:main']
      },
      classifiers=[
          "License :: OSI Approved :: MIT License",
          'Intended Audience :: Developers',
          'Development Status :: 3 - Alpha',
          'Programming Language :: Python :: 2.7',
          'Programming Language :: Python :: 3.6',
          'Topic :: Database :: Front-Ends',
      ],
)
