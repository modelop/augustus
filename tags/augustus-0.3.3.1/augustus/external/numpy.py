"""Augustus requires external package: numpy

Numpy is available for download from
<http://numpy.scipy.org/>

"""

__path__ = None
try:
  import numpy
except ImportError,e:
  e.args = (e.args[0]+'\n\n'+__doc__,)
  raise
__path__ = numpy.__path__
__all__ = numpy.__all__
from numpy import *
del numpy


#################################################################
# vim:sw=2:sts=2:expandtab:shiftround
