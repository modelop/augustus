
"""Augustus external package import module

1. All imports from Augustus of external packages (not part of the
standard python distribution) should be done through this module.

For example:
  from augustus.external import pygsl

This serves several purposes:
  - makes external dependencies explicit
  - provides meaningful error messages for unavailable packages
    including instructions on what to download and install
  - provides a hook for handling site-specific library locations
  - makes possible use of a central config file to select
    from version and package alternatives
  - helps make version and package migration transparent, eg
      python2.4 --> python2.5
      numarray --> numpy

2. The numerix package should be used in place of numarray.
By default it just imports numarray, but it provides the
ability to switch the numerical package based on an environment
variable: NUMERIX=[numarray|numpy|numpy.numarray]
This simplifies future migration and regression testing, and allows 
for easy temporary override for special purposes like gaining
access to arrays larger than 2GB.

For example, instead of:
  import numarray as na
use something like:
  from augustus.external import numerix as nx

Currently only python2.4 and NUMERIX=numarray are officially supported,
but for testing and special needs like array allocations over 2GB
tools can be invoked using "NUMERIX=numpy.numarray python2.5",
for example.

3. The etree package should be used to import ElementTree.
It loads the faster CElementTree if available and
falls back to the pure python ElementTree otherwise.
This is the preferred way to use elementtree in Augustus.
For example:
  from augustus.external.etree import Element, SubElement

"""

# to test site installation, try 'from augustus.external import *'
# do not use the wildcard import for normal programs

__xall__ = (
  'etree',
  'elementtree',
  'cElementTree',
  'lxml',

  'numerix',
  'numpy',

  'pygsl',
)

#################################################################
# vim:sw=2:sts=2:expandtab:shiftround
