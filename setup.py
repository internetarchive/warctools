#!/usr/bin/env python

import os
from setuptools import setup

setup(name='warctools',
    version="4.10.0",
    license="MIT License",
    description='Command line tools and libraries for handling and manipulating WARC files (and HTTP contents)',
    author='Thomas Figg',
    author_email='tef@warctools.twentygototen.org',
    packages=['hanzo', 'hanzo.warctools','hanzo.httptools'],
    test_suite="nose.collector",
    tests_require=["nose"],
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
        warcpayload=hanzo.warcpayload:run
    """,
    )
