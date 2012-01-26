#!/usr/bin/env python

import glob 

from setuptools import setup

setup(name='hanzo-warc-tools',
      version='0.2',
      license="MIT License",
      description='Command line tools and libraries for handling and manipulating WARC files (and HTTP contents)',
      author='Thomas Figg',
      author_email='thomas.figg@hanzoarchives.com',
      packages=['hanzo.warctools','hanzo.httptools'],
      scripts=glob.glob('*warc*.py'),
     )

