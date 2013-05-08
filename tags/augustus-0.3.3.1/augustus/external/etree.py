"""Python ElementTree package loader

This loads the faster CElementTree if available and
falls back to the pure python ElementTree otherwise.

This is the preferred way to use elementtree in Augustus.
For example:
  from augustus.external.etree import Element, SubElement

"""

try:
  from cElementTree import ElementInclude,ElementTree,ElementPath
except ImportError:
  #picks up modules in elementtree *package*:
  from elementtree import ElementInclude,ElementTree,ElementPath
  
#################################################################
# vim:sw=2:sts=2:expandtab:shiftround
