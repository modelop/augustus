"""Python numerical package loader

This is a virtual package intended to support transparent
switching of the underlying numerical package import.

Setting the environment variable NUMERIX selects
which package is imported.

Valid values are: 'numarray', 'numpy', and 'numpy.numarray'

"""

__path__ = None
import os
__which = os.getenv('NUMERIX','numarray')
del os

if __which == 'numarray':
  import numarray
  __path__ = numarray.__path__
  from numarray import ma
  from numarray import *
  del numarray
elif __which == 'numpy':
  import numpy
  __path__ = numpy.__path__
  __all__ = numpy.__all__
  from numpy import *
  del numpy
elif __which == 'numpy.numarray':
  import numpy.numarray
  __path__ = numpy.numarray.__path__
  __all__ = numpy.numarray.__all__
  from numpy.numarray import *
  del numpy
else:
  raise ImportError('NUMERIX=%r is not supported\n\n%s' % (which,__doc__))

del __which

#################################################################
# vim:sw=2:sts=2:expandtab:shiftround
