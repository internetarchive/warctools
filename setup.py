#!/usr/bin/env python

import glob 

from setuptools import setup

setup(name='hanzo-warc-tools',
      version='0.1',
      description='command line tools and libraries for handling and manipulating WARC files',
      author='Thomas Figg',
      author_email='thomas.figg@hanzoarchives.com',
      packages=['hanzo','hanzo.warctools','hanzo.httptools'],
      scripts=glob.glob('*warc*.py'),
     )

