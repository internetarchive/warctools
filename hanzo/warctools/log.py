from __future__ import print_function

import sys

__all__ = ['debug']

if __debug__:
    def debug(*args):
        print('WARCTOOLS', args, file=sys.stderr)
else:
    def debug(*args):
        pass
    
