"""
These veclib functions are mainly convenience methods that:
- provide wrapper access to library functions that normally
  operate in-place
- simplify use of common combinations, eg. for one-liners in rules

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

import division
import itertools as it
from base import corelib, EquivUnary, Test, as_any_array, as_num_array
subtract = corelib.subtract
minimum = corelib.minimum
maximum = corelib.maximum
clip = corelib.clip
where = corelib.where

########################################################################

class RangeCap(EquivUnary):
  '''cap extreme values at specified limits

    >>> func = RangeCap()
    >>> assert func([1,3,-9,-3,6,7,9],lower=-5,upper=6).tolist() == [1,3,-5,-3,6,6,6]

  '''
  name = 'range_cap'
  ranking = ('naive','clip')

  tests = (
    Test([1,3,-9,-3,6,7,9,0],lower=None,upper=None) == [1,3,-9,-3,6,7,9,0],
    Test([1,3,-9,-3,6,7,9,0],lower=5,upper=None) == [5,5,5,5,6,7,9,5],
    Test([1,3,-9,-3,6,7,9,0],lower=-5,upper=6) == [1,3,-5,-3,6,6,6,0],
  )

  @staticmethod
  def clip(arg,lower=None,upper=None):
    arg = as_num_array(arg)
    if lower is not None and upper is not None:
      arg = clip(arg,lower,upper)
    else:
      if lower is not None:
        arg = maximum(arg,lower)
      if upper is not None:
        arg = minimum(arg,upper)
    return arg

  @staticmethod
  def naive(arg,lower=None,upper=None):
    arg = as_num_array(arg)
    if lower is not None:
      arg = maximum(arg,lower)
    if upper is not None:
      arg = minimum(arg,upper)
    return arg


########################################################################

class RoundAll(EquivUnary):
  '''apply python round function (there is no equivalent ufunc)

    >>> func = RoundAll()
    >>> assert func([12.3456,30,456.12,0],ndigits=2).tolist() == [12.35,30.,456.12,0.]

  '''
  name = 'round_all'
  ranking = ('roundint','naive')

  tests = (
    Test([12.3456,30,456.12,0],ndigits=-1) == [10,30,460,0],
    Test([12.3456,30,456.12,0],ndigits=0) == [12,30,456,0],
    Test([12.3456,30,456.12,0],ndigits=1) == [12.3,30,456.1,0],
    Test([12.3456,30,456.12,0],ndigits=2) == [12.35,30,456.12,0],
    Test([-12.3456,-30,-456.12,0],ndigits=2) == [-12.35,-30,-456.12,0],
  )

  @staticmethod
  def roundint(arg,ndigits=0):
    arg = as_num_array(arg)
    factor = 10**ndigits
    adjust = where(arg>=0,0.5,-0.5)
    out = ((arg*factor)+adjust).astype('Int') / float(factor)
    return out

  @staticmethod
  def naive(arg,ndigits=0):
    arg = as_num_array(arg)
    out = [round(val,ndigits) for val in arg]
    return as_num_array(out)

########################################################################

if __name__ == "__main__":
  from base import tester
  tester.testmod()

########################################################################
