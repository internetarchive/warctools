#!/usr/bin/env python

import glob

from setuptools import setup

fh = open("version", "rb")
version = fh.readline()
fh.close()
version.strip()

setup(name='hanzo-warc-tools',
      version=version,
      license="MIT License",
      description='Command line tools and libraries for handling and manipulating WARC files (and HTTP contents)',
      author='Thomas Figg',
      author_email='thomas.figg@hanzoarchives.com',
      packages=['hanzo', 'hanzo.warctools','hanzo.httptools'],
      scripts=glob.glob('*warc*.py'),
      namespace_packages=["hanzo"],
      test_suite="hanzo.httptools.tests",
      test_loader="unittest2:TestLoader"
     )
