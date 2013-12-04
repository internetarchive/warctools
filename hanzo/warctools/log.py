import sys

from six import print_

__all__ = ['debug']

if __debug__:
    def debug(*args):
        print_('WARCTOOLS',args, file=sys.stderr)
else:
    def debug(*args):
        pass
    
