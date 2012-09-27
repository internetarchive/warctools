#!/usr/bin/env python

import glob

from setuptools import setup

fh = open("version", "rb")
version = fh.readline()
fh.close()
version.strip()

setup(name='hanzo-warctools',
    version=version,
    license="MIT License",
    description='Command line tools and libraries for handling and manipulating WARC files (and HTTP contents)',
    author='Thomas Figg',
    author_email='thomas.figg@hanzoarchives.com',
    packages=['hanzo', 'hanzo.warctools','hanzo.httptools'],
#    namespace_packages=["hanzo"],
    test_suite="hanzo.httptools.tests",
    test_loader="unittest2:TestLoader",
    entry_points="""
        [console_scripts]
        warcdump=hanzo.warcdump:run
        arc2warc=hanzo.arc2warc:run
        warcextract=hanzo.warcextract:run
        warcfilter=hanzo.warcfilter:run
        warcindex=hanzo.warcindex:run
        warclinks=hanzo.warclinks:run
        warcvalid=hanzo.warcvalid:run
        warc2warc=hanzo.warc2warc:run
    """,
    )
