# TODO: Is this used?

"""This is a wrapper providing functionality that should really have
  been in numarray.  This is not for math or data functions,  just
  admin stuff like type management, etc.

  Comprehensive conversion to numarray arrays
  >>> as_any_array([1,2,3])
  array([1, 2, 3])
  >>> as_any_array([1,2.3,4])
  array([ 1. ,  2.3,  4. ])
  >>> as_any_array([1.9,2.3,4.2])
  array([ 1.9,  2.3,  4.2])
  >>> as_any_array(['1','2','3'])
  CharArray(['1', '2', '3'])
  >>> as_any_array(['a','b','cd'])
  CharArray(['a', 'b', 'cd'])
  >>> as_any_array(['def'])
  CharArray(['def'])
  >>> as_any_array('def')
  CharArray(['def'])

  >>> import_native(['1','2','3'])
  array([1, 2, 3], type=Int8)
  >>> import_native(['1','2.3','4'])
  array([ 1. ,  2.3,  4. ])
  >>> import_native(['1.9','2.3','4.2'])
  array([ 1.9,  2.3,  4.2])
  >>> import_native(['a',2.3,4.2])
  CharArray(['a', '2.3', '4.2'])
  >>> import_native(['a','2.3','4'])
  CharArray(['a', '2.3', '4'])
  >>> import_native(as_any_array([1,2,3]))
  array([1, 2, 3])
  >>> import_native(as_any_array(['a','2.3','4']))
  CharArray(['a', '2.3', '4'])
  >>> import_native(as_any_array([2.3,'2.3','']))
  CharArray(['2.3', '2.3', ' '])

"""

__copyright__ = """
Copyright (C) 2005-2007  Open Data ("Open Data" refers to
one or more of the following companies: Open Data Partners LLC,
Open Data Research LLC, or Open Data Capital LLC.)

This file is part of Augustus.

Augustus is free software; you can redistribute it and/or
modify it under the terms of the GNU General Public License
as published by the Free Software Foundation; either version 2
of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program; if not, write to the Free Software
Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA
"""


__all__ = (
  'as_any_array','as_num_array','as_char_array',
  'is_any_array','is_num_array','is_char_array',
  'import_native','compact','get_format','get_bestfit',
)

########################################################################
# contain all references to underlying implementation

from augustus.external import numpy

as_num_array = numpy.asarray
as_char_array = numpy.chararray
base_any_array = numpy.ndarray

########################################################################

def is_sequence(x): return isinstance(x,(list,tuple))
def is_scalar(x): return isinstance(x,(bool,int,long,float,complex,str))
def is_any_array(x): return isinstance(x,base_any_array)

# JP 2011-03-01: should is_num_array return True for a complex array?  If so, change "Float" to "AllFloat" below...
def is_num_array(x): return isinstance(x,base_any_array) and x.dtype.char in numpy.typecodes["Float"] + numpy.typecodes["AllInteger"]
# JP 2011-03-01: "S" (string) and "U" (unicode) are strangely not classified in numpy.typecodes
def is_char_array(x): return isinstance(x,base_any_array) and x.dtype.char in numpy.typecodes["Character"] + "SU"

def all_scalar(args):
  for x in args:
    if not is_scalar(x):
      return 0
  return len(args) > 0

def all_arrays(args):
  for x in args:
    if not is_any_array(x):
      return 0
  return len(args) > 0

########################################################################

def as_any_array(data):
  '''ensure data is an array object, handling numeric, string, and mixed data
  '''
  if is_any_array(data):
    return data
  # strings are not taken apart
  if isinstance(data,str):
    return as_char_array([data])
  # needs conversion, first try as numbers
  try:
    return as_num_array(data)
  except:
    pass
  # try as pure strings
  try:
    return as_char_array(data)
  except:
    pass
  # mixed data, force everything to string type
  if not is_sequence(data):
    data = [data]
  return as_char_array([str(value) for value in data])

########################################################################

def import_native(data):
  '''ensure array object, converting string arrays to numeric if possible
  '''
  out = as_any_array(data)
  if is_num_array(out):
    return out
  # try for a clean conversion to numeric form
  # must try int before float because float succeeds on ints
  try:
    newdata = map(int,data)
  except ValueError:
    try:
      newdata = map(float,data)
    except ValueError:
      newdata = None
  if newdata is not None:
    # pure numeric
    out = compact(as_num_array(newdata))
  else:
    # not pure numeric, so ensure all strings
    newdata = map(str,data)
    out = as_char_array(newdata)
  return out

#################################################################

def compact(arr):
  '''compact storage for array, if possible

  >>> x = as_num_array([1234,45,888])
  >>> print get_format(x), get_format(compact(x))
  Int32 Int16
  '''

  oformat = get_format(arr)
  nformat = get_bestfit(arr)
  if oformat != nformat:
    return arr.astype(nformat)
  return arr

#################################################################

def get_format(arr):
  '''return RecordArray format code for given array

  >>> i = as_num_array([0,1])
  >>> s = as_char_array(('abc','defg'))
  >>> print get_format(i), get_format(s)
  Int32 a4
  
  '''
  try:
    out = str(arr.type())
  except AttributeError:
    assert is_char_array(arr), 'cannot determine format: %s' % type(arr)
    out = 'a%s' % arr.itemsize()
  return out

#################################################################

_type_ranges = {
	'Bool':			(0,1),
	'Int8':			(-0x80,0x7f),
	'Int16':		(-0x8000,0x7fff),
	'Int32':		(-0x80000000,0x7fffffff),
	'Int64':		(-0x8000000000000000,0x7fffffffffffffff),
	'UInt8':		(0x0,0xff),
	'UInt16':		(0x0,0xffff),
	'UInt32':		(0x0,0xffffffff),
	'UInt64':		(0x0,0xffffffffffffffff),
}


def get_bestfit(arr):
  '''return smallest compatible format code for given array

  >>> x = as_num_array([1234,45,888])
  >>> print x.type(), get_bestfit(x)
  Int32 Int16

  '''
  format = get_format(arr)
  if format[:3] == 'Int':
    check = ['Bool','Int8','Int16','Int32']
  elif format[:4] == 'UInt':
    check = ['Bool','UInt8','UInt16','UInt32']
  elif format[:5] == 'Float':
    #TODO: Floats, tricky - need to look for small decimals and fractions
    #return 'Float32' - this breaks many tests by adding fuzz at the wee digits
    return format
  else:
    #TODO: CharArray squeeze
    return format
  xmin = arr.min()
  xmax = arr.max()
  for nformat in check:
    (minlim,maxlim) = _type_ranges[nformat]
    if (xmin >= minlim) and (xmax <= maxlim):
      return nformat
  return format

#################################################################
#################################################################

if __name__ == "__main__":
  import doctest
  doctest.testmod()

#################################################################
