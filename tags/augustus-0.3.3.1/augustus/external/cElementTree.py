"""Augustus requires external package: cElementTree

For python version >= 2.5 this is part of the standard library.
For earlier python versions this is available from
<http://effbot.org/zone/celementtree.htm>

"""

__path__ = None
import sys
if sys.version_info >= (2,5):
  from xml.etree.cElementTree import *
else:
  try:
    from cElementTree import *
  except ImportError,e:
    e.args = (e.args[0]+'\n\n'+__doc__,)
    raise
del sys, __path__

#################################################################
# vim:sw=2:sts=2:expandtab:shiftround
