"""Augustus requires external package: elementtree

For python version >= 2.5 this is part of the standard library.
For earlier python versions this is available from
<http://effbot.org/zone/elementtree.htm>

"""

__path__ = None
import sys
if sys.version_info >= (2,5):
  from xml.etree import ElementInclude, ElementPath, ElementTree
else:
  try:
    from elementtree import ElementInclude, ElementPath, ElementTree 
  except ImportError,e:
    e.args = (e.args[0]+'\n\n'+__doc__,)
    raise
del sys, __path__

#################################################################
# vim:sw=2:sts=2:expandtab:shiftround
