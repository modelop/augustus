
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


import itertools as it
from base import corelib, EquivUnary, Test, as_any_array, as_num_array
ones = corelib.ones
arange = corelib.arange
groupby = it.groupby
izip = it.izip
takewhile = it.takewhile
dropwhile = it.dropwhile


########################################################################

class UniqueMask(EquivUnary):
  '''given sorted data, return mask selecting unique values.
    Typically used to reduce data with identical timestamps.
    By default, last value from a run of equal values is taken.

  '''
  name = 'unique_mask'
  ranking = ('fast','naive_loop',)

  tests = (
    Test([0])				== [1],
    Test([0],first=True)		== [1],
    Test([1,2,3,7,8,9])			== [1,1,1,1,1,1],
    Test([1,2,3,7,8,9],first=True)	== [1,1,1,1,1,1],
    Test([1,3,3,3,4,5])			== [1,0,0,1,1,1],
    Test([1,3,3,3,4,5],first=True)	== [1,1,0,0,1,1],
    Test([1,1,3,3,5,5],first=False)	== [0,1,0,1,0,1],
    Test([1,1,3,3,5,5],first=True)	== [1,0,1,0,1,0],
  )

  @staticmethod
  def naive_loop(arg,first=False):
    arg = as_num_array(arg)
    if first:
      out = [1]
      for i in xrange(len(arg)-1):
        if arg[i] != arg[i+1]:
          out.append(1)
        else:
          out.append(0)
    else:
      out = []
      for i in xrange(1,len(arg)):
        if arg[i] != arg[i-1]:
          out.append(1)
        else:
          out.append(0)
      out.append(1)
    return as_num_array(out,type='Bool')

  @staticmethod
  def fast(arg,first=False):
    arg = as_num_array(arg)
    out = ones(len(arg),type='Bool')
    if first:
      reject = arg[1:] == arg[:-1]
      out[1:] -= reject
    else:
      reject = arg[:-1] == arg[1:]
      out[:-1] -= reject
    return out


########################################################################

if __name__ == "__main__":
  from base import tester
  tester.testmod()

########################################################################
