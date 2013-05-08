"""This is a wrapper providing functionality that should really have
  been in numpy.  This is not for math or data functions,  just
  admin stuff like type management and reconciling the differences
  between numeric, string, and masked arrays.

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

########################################################################

__all__ = (
  'asarray', 'import_asarray',
  'export_string', 'pack_binary_mask', 'unpack_binary_mask',
  'compact', 'get_format', 'get_bestfit',
  'is_any_array', 'is_num_array', 'is_char_array', 'is_masked_array',
  'as_num_array', 'as_char_array', 'as_masked_array', 'count_masked',
  'any_compress',
)

from numpy import NAN
import numpy as na
import numpy.ma as ma
from numpy import char as nastr

base_ndarray = na.ndarray
base_num_array = na.ndarray
base_char_array = nastr.chararray
any_compress = na.compress

base_masked_array = ma.MaskedArray
nonzero = na.nonzero
alltrue = na.alltrue

as_num_array = na.asarray
as_char_array = nastr.asarray
as_masked_array = ma.array

def is_sequence(x): return isinstance(x,(list,tuple))
def is_scalar(x): return isinstance(x,(bool,int,long,float,complex,str))

def is_any_array(x): return isinstance(x,(base_ndarray,base_masked_array))
def is_num_array(x): return isinstance(x,(base_num_array,base_masked_array))
def is_char_array(x):
  return isinstance(x,base_char_array) or x.dtype.char == 'S'
def is_masked_array(x): return isinstance(x,base_masked_array)

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

def count_masked(x):
  '''return number of masked values in given array'''
  if is_masked_array(x):
    return len(x)-x.count()
  return 0

########################################################################

def asarray(data,trymasked=False,masked_values=('',)):
  """unifying wrapper to asarray() function handling any data type
  
  Input may be numeric, string, or mixed data.  Output may be a
  numeric array, a string array, or a masked numeric array.
  This does not attempt to convert strings to numbers, just to
  ensure that the data is an array.

  >>> asarray(1)
  array(1)
  >>> asarray([1])
  array([1])
  >>> asarray([1,2,3])
  array([1, 2, 3])
  >>> asarray([1,2.3,4])
  array([ 1. ,  2.3,  4. ])
  >>> asarray([1.9,2.3,4.2])
  array([ 1.9,  2.3,  4.2])
  >>> asarray(['1','2','3'])
  CharArray(['1', '2', '3'])
  >>> asarray(['a','b','cd'])
  CharArray(['a', 'b', 'cd'])
  >>> asarray(['def'])
  CharArray(['def'])
  >>> asarray('def')
  CharArray(['def'])
  >>> print asarray([1,None,2,3],trymasked=True)
  [1 -- 2 3]
  >>> print asarray([1,'',2.5],trymasked=True)
  [1.0 -- 2.5]
  >>> asarray([1,None,'2.5'],trymasked=True)
  CharArray(['1', ' ', '2.5'])

  """
  if is_any_array(data):
    return data

  # needs conversion, try high runner cases first

  # avoid interpreting a string as a sequence
  # (but allow bare number to be taken as rank-0 array)
  if isinstance(data,basestring):
    data = [data]

  # try as pure numbers
  try:
    a = as_num_array(data)
    return a
  except:
    pass

  # try as pure string
  # TODO - I don't think this code will be reached anymore:
  try:
    out = as_char_array(data)
  except:
    out = None
  if out is not None:
    return out

  if trymasked:
    # pure conversions have failed, consider masked numeric
    if isinstance(masked_values,basestring):
      masked_values = [masked_values]
    else:
      masked_values = list(masked_values)
    if None not in masked_values:
      masked_values.append(None)

    if not is_sequence(data):
      data = [data]
    mdata = data[:]
    mmask = [0]*len(mdata)
    for i,val in enumerate(mdata):
      if isinstance(val,(int,long,float,complex)):
        continue
      elif val in masked_values:
        mdata[i] = 0
        mmask[i] = 1
      else:
        # not masked numeric, short circuit conversion
        mdata = mmask = None
        break
    if mdata is not None:
      out = as_masked_array(mdata,mask=mmask)
      return out

  # mixed data, force everything to string type
  def str_none(val,default=''):
    if val is None: return default
    return str(val)
  out = as_char_array([str_none(fld) for fld in data])
  return out

########################################################################

def import_asarray(data,noconvert=False,trymasked=False,masked_values=('',)):
  """given an array or a list convertable to an array, try converting
  data to an appropriate numeric array
  
  >>> import_asarray(['1','2','3'])
  array([1, 2, 3], dtype=int8)
  >>> import_asarray(['1','2.3','4'])
  array([ 1. ,  2.3,  4. ])
  >>> import_asarray(['1.9','2.3','4.2'])
  array([ 1.9,  2.3,  4.2])
  >>> import_asarray(['a',2.3,4.2])
  CharArray(['a', '2.3', '4.2'])
  >>> import_asarray(['a','2.3','4'])
  CharArray(['a', '2.3', '4'])
  >>> import_asarray(asarray([1,2,3]))
  array([1, 2, 3])
  >>> import_asarray(asarray(['a','2.3','4']))
  CharArray(['a', '2.3', '4'])
  >>> import_asarray(asarray([2.3,'2.3a','']))
  CharArray(['2.3', '2.3a', ' '])
  >>> print import_asarray(['1',None,'3'],trymasked=True)
  [1 -- 3]

  """
  out = asarray(data,trymasked=trymasked,masked_values=masked_values)
  if not is_char_array(out) or noconvert:
    return out
  
  # try for a clean conversion to numeric form
  if not trymasked:
    need_mask = False
  else:
    # is masked array needed?
    if isinstance(masked_values,basestring):
      masked_values = [masked_values]
    mask = (out != out)   # base mask
    for mval in masked_values:
      mask |= (out == mval)
    need_mask = mask.sum()

  if need_mask:
    # replace maskable elements with number
    tmp = out.copy()
    tmp[mask] = '0'
  else:
    tmp = out

  # must try int before float because float succeeds on ints
  for xtype in (int,float):
    try:
      newdata = [xtype(val) for val in tmp]
      break
    except ValueError:
      newdata = None

  # if conversion not possible, return the string-array
  if newdata is None:
    return out
  if need_mask:
    out = as_masked_array(newdata,mask=mask)
  else:
    out = as_num_array(newdata)

  # compact storage
  out = compact(out)
  return out

#################################################################

def export_string(data,fill_value=''):
  '''prepare exportable array object, handling masked arrays

  Returns array suitable for string output (eg. for CSV files)
  such that applying the str() function to each element yields
  external representation.  Specifically, non-masked arrays are
  returned as-is, and masked arrays return a string array with
  appropriate masked value representations.
  

  >>> x = asarray([0,-1,1,2])
  >>> print export_string(x)
  [ 0 -1  1  2]
  >>> print export_string(x.astype('float'))
  [ 0. -1.  1.  2.]
  >>> y = ma.masked_array(x,[1,0,0,0])
  >>> print export_string(y)
  [' ' '-1' '1' '2']
  >>> z = ma.masked_array(x,[0,0,1,0]).astype('int8')
  >>> print export_string(z)
  ['0' '-1' ' ' '2']

  '''
  arr = asarray(data)
  if not is_masked_array(arr):
    return arr
  # avoid special handling if nothing is masked
  arr.unmask()
  data = arr.compressed().filled()
  if arr.mask() is None:
    return data
  # at least one element is masked
  out = as_char_array([str(val) for val in arr.filled(0)])
  out[arr.mask() != 0] = fill_value
  return out

########################################################################

# integer divergence limit where Int32 != Float32
_ieee_range32 = (-268450016,268450016)

def pack_binary_mask(data,use_nan=False,fill_value=None):
  '''Given a masked array, encode the mask information in the
  array, using one of two methods, and return a 'masktype' code
  along with the packed array.  The 'masktype' must be provided
  to unpack the masked array.  This is intended for storage
  of the array.

  The default method is to keep the native array type and to choose
  an unused value in the current data to represent the mask positions.
  In this case, the returned masktype is a numeric value corresponding
  to the mask value.

  When the 'use_nan' argument is given, masked values are represented
  as IEEE floating point NAN values.  Integers are also handled the
  same way, taking care to preserve the precision.  The returned
  masktype is the true array type of the data (eg 'Int16'), which is
  to be restored after building the mask from the IEEE NAN values.
  Note that this approach, while being 'numerically correct', sometimes
  raises warnings even on correct conversions, and (according to
  comments on the newsgroups) may have portability problems.

  Returns tuple (masktype,exportable_array).

  >>> x = asarray([0,-1,1,2])
  >>> print pack_binary_mask(x)
  (None, array([ 0, -1,  1,  2]))
  >>> print pack_binary_mask(x.astype('Float'))
  (None, array([ 0., -1.,  1.,  2.]))

  >>> y = ma.masked_array(x,[1,0,0,0])
  >>> print pack_binary_mask(y)
  (0, array([ 0, -1,  1,  2]))
  >>> print pack_binary_mask(y,use_nan=1)
  ('Int32', array([        nan, -1.        ,  1.        ,  2.        ], type=Float32))

  >>> z = ma.masked_array(x,[0,0,1,0]).astype('Int8')
  >>> print pack_binary_mask(z)
  (-128, array([   0,   -1, -128,    2], type=Int8))
  >>> print pack_binary_mask(z,use_nan=1)
  ('Int8', array([ 0.        , -1.        ,         nan,  2.        ], type=Float32))

  >>> z = ma.masked_array(x,[0,0,1,0]).astype('UInt8')
  >>> print pack_binary_mask(z)
  (254, array([  0, 255, 254,   2], type=UInt8))
  >>> print pack_binary_mask(z,use_nan=1)
  ('UInt8', array([ 0.        , 255.        ,         nan,  2.        ], type=Float32))


  '''
  arr = asarray(data)
  if not is_masked_array(arr):
    return None,arr
  # avoid special handling if nothing is masked
  arr.unmask()
  data = arr.compressed().filled()
  if arr.mask() is None:
    return None,data
  # at least one element is masked
  oformat = get_format(arr)
  # no special handling needed for Float
  if oformat[:5] == 'Float':
    return oformat,arr.filled(NAN)
  # remaining types are some flavor of Int

  # for nan-type masking, convert value safely
  # to Float and encode mask using NAN
  # returned mask_type = original format code
  if use_nan:
    # is it safe to use Float32?
    minlim,maxlim = _ieee_range32
    if (min(data) >= minlim) and (max(data) <= maxlim):
      extype = 'Float32'
    else:
      extype = 'Float64'
    return oformat, arr.astype(extype).filled(NAN)

  # for value-type masking, keep original data type
  # and select unused value as mask indicator
  # (unless explicitly provided by caller)
  # returned mask_type = numeric fill_value
  if fill_value is None:
    # examine non-masked data without redundancies
    bytes = data.itemsize()
    data = set(data)
    # give preference to zero
    if 0 not in data:
      fill_value = 0
    else:
      span = 1 << (bytes*8)
      if oformat[:1] == 'U':
        candidates = xrange(span-1,-1,-1)
      else:
        candidates = xrange(-span/2,span/2)
      for n in candidates:
        if n not in data:
          fill_value = n
          break
      assert fill_value is not None and fill_value not in data, \
        'unable to select fill_value'
  # for value-type masking, mask_type = fill_value
  return fill_value,arr.filled(fill_value)

################################
################################

def unpack_binary_mask(data,masktype):
  """restores a masked array from the pack_binary_mask function
  described above.

  >>> def roundtrip(data,**kwargs):
  ...   x1 = asarray(data)
  ...   masktype,x2 = pack_binary_mask(x1,**kwargs)
  ...   x3 = unpack_binary_mask(x2,masktype)
  ...   if not is_masked_array(x1):
  ...     assert alltrue(x1 == x3)
  ...   else:
  ...     assert alltrue(x1.mask() == x3.mask())
  ...     assert alltrue(x1.compressed() == x3.compressed())
  ...   print 'OK: %s stored as %s' % (x1,x2)


  >>> x = [0,-1,1,2]
  >>> roundtrip(x)
  OK: [ 0 -1  1  2] stored as [ 0 -1  1  2]

  >>> y = ma.masked_array(x,mask=[1,0,0,0])
  >>> roundtrip(y)
  OK: [-- -1 1 2] stored as [ 0 -1  1  2]

  >>> roundtrip(y,use_nan=1)
  Warning: Encountered invalid numeric result(s)  during type conversion
  OK: [-- -1 1 2] stored as [        nan -1.          1.          2.        ]

  >>> z = ma.masked_array(x,[0,0,1,0]).astype('Int8')
  >>> roundtrip(z)
  OK: [0 -1 -- 2] stored as [   0   -1 -128    2]
  >>> roundtrip(z,use_nan=1)
  Warning: Encountered invalid numeric result(s)  during type conversion
  OK: [0 -1 -- 2] stored as [ 0.         -1.                 nan  2.        ]

  >>> z = ma.masked_array(x,[0,0,1,0]).astype('UInt8')
  >>> roundtrip(z)
  OK: [0 255 -- 2] stored as [  0 255 254   2]
  >>> roundtrip(z,use_nan=1)
  Warning: Encountered invalid numeric result(s)  during type conversion
  OK: [0 255 -- 2] stored as [ 0.         255.                 nan  2.        ]

  >>> f = ma.masked_array([2.5,-9,0.0,1.0],mask=[0,0,1,1])
  >>> roundtrip(f)
  OK: [2.5 -9.0 -- --] stored as [ 2.5        -9.                 nan         nan]
  >>> roundtrip(f,use_nan=1)
  OK: [2.5 -9.0 -- --] stored as [ 2.5        -9.                 nan         nan]

  """
  arr = asarray(data)
  if masktype in (None,''):
    return arr
  if not is_num_array(arr) or is_masked_array(arr):
    raise TypeError, 'cannot unpack array type: %s' % type(arr)
  for xtype in (int,float):
    try:
      fill_value = xtype(masktype)
      break
    except ValueError:
      fill_value = None
  if fill_value is not None:
    # default mask encoding using given fill_value
    mask = (arr == fill_value)
    out = as_masked_array(arr,mask=mask)
    return out
  # otherwise masktype is final array type, mask value is IEEE NAN
  mask = (arr != arr)   # peculiar characteristic of NAN
  out = as_masked_array(arr,mask=mask)
  out.set_fill_value()   # avoid type conversion warnings on the masked data
  return out.astype(masktype)


#################################################################
#################################################################

def compact(arr,also_floats=False):
  '''compact storage for array, if possible

  >>> x = as_num_array([1234,45,88])
  >>> print get_format(x), get_format(compact(x))
  int32 int16
  >>> y = ma.array(x,mask=[1,0,0])
  >>> print get_format(y), get_format(compact(y))
  int32 int8

  '''

  oformat = get_format(arr)
  nformat = get_bestfit(arr,also_floats=also_floats)
  if oformat != nformat:
    return arr.astype(nformat)
  return arr

#################################################################

def get_format(arr):
  '''return RecordArray format code for given array

  >>> i = as_num_array([0,1])
  >>> s = as_char_array(['abc','defg'])
  >>> m = ma.array([2.3,-1],mask=[0,1])
  >>> print get_format(i), get_format(s), get_format(m)
  int32 a4 float64
  
  '''
  if arr is None:
    return None
  if is_char_array(arr):
    return 'a%s' % arr.itemsize
  return str(arr.dtype)

#################################################################

_type_ranges = {
	'bool_':		(0,1),
	'int8':			(-0x80,0x7f),
	'int16':		(-0x8000,0x7fff),
	'int32':		(-0x80000000,0x7fffffff),
	'int64':		(-0x8000000000000000,0x7fffffffffffffff),
	'uint8':		(0x0,0xff),
	'uint16':		(0x0,0xffff),
	'uint32':		(0x0,0xffffffff),
	'uint64':		(0x0,0xffffffffffffffff),
}


def get_bestfit(arr,also_floats=False):
  '''return smallest compatible format code for given array

  >>> x = as_num_array([1234,45,888])
  >>> print get_format(x), get_bestfit(x)
  int32 int16
  >>> y = ma.array([12,45555555,88],mask=[0,1,0])
  >>> print get_format(y), get_bestfit(y)
  int32 int8

  '''

  format = get_format(arr)
  if format[:3] == 'int':
    check = ['bool_','int8','int16','int32']
  elif format[:4] == 'uint':
    check = ['bool_','uint8','uint16','uint32']
  elif format[:5] == 'float':
    #TODO: Floats, tricky - need to look for small decimals and fractions
    #return 'Float32' - this breaks many tests by adding fuzz at the wee digits
    if also_floats:
      return 'float32'
    return format
  else:
    #TODO: CharArray squeeze
    return format
  if is_masked_array(arr):
    # masked arrays do not have min(), max()
    arr = ma.filled(arr)
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
  flags =  doctest.NORMALIZE_WHITESPACE
  flags |= doctest.ELLIPSIS
  flags |= doctest.REPORT_ONLY_FIRST_FAILURE
  doctest.testmod(optionflags=flags)

