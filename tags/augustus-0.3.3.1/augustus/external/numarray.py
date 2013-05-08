"""Augustus requires external package: numarray

Numarray is available for download from
<http://www.stsci.edu/resources/software_hardware/numarray>

"""

__path__ = None
try:
  import numarray as _nx
except ImportError,e:
  e.args = (e.args[0]+'\n\n'+__doc__,)
  raise
from numarray import __version__
from numarray import *
from numarray import records
from numarray import strings
from numarray import objects
from numarray import ma
from numarray import random_array
from numarray import ieeespecial
__path__ = _nx.__path__

#################################################################
# vim:sw=2:sts=2:expandtab:shiftround
