import sys

__all__ = ['debug']

debug = lambda:None

if __debug__:
    def debug(*args):
        print >> sys.stderr, 'WARCTOOLS',args
