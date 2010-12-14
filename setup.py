#!/usr/bin/env python

import glob 

from distutils.core import setup

setup(name='hanzo-warc-tools',
      version='0.1',
      description='command line tools and libraries for handling and manipulating WARC files',
      author='Thomas Figg',
      author_email='thomas.figg@hanzoarchives.com',
      packages=['warctools'],
      scripts=glob.glob('*warc*.py'),
     )

