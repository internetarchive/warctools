#!/usr/bin/env python

'''
internetarchive/warctools setup
'''

from setuptools import setup

setup(
    author='Thomas Figg',
    author_email='tef@warctools.twentygototen.org',
    classifiers=[
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
        'Programming Language :: Python :: 3.5',
        'Topic :: System :: Archiving',
    ],
    description='Command line tools and libraries for handling and manipulating WARC files (and HTTP contents)',
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
    maintainer='Internet Archive',
    maintainer_email='info@archive.org',
    name='warctools',
    packages=['hanzo', 'hanzo.warctools', 'hanzo.httptools'],
    test_suite="nose.collector",
    tests_require=["nose"],
    version='5.0.0',
)
