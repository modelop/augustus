"""Augustus requires external package: lxml

Lxml is available for download from
<http://codespeak.net/lxml/>

"""

__path__ = None
try:
  import lxml
except ImportError,e:
  e.args = (e.args[0]+'\n\n'+__doc__,)
  raise
__path__ = lxml.__path__
del lxml

#################################################################
# vim:sw=2:sts=2:expandtab:shiftround
