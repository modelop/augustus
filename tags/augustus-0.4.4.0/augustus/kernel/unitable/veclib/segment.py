"""Segmentation

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

import itertools as it
import division
from base import corelib, Test, EquivUnary, EquivBinary, as_num_array
searchsorted = corelib.searchsorted
arange = corelib.arange
zeros = corelib.zeros

########################################################################

class Discretize(EquivUnary):
  """Discretize according to sorted segment table

  >>> func = Discretize()
  >>> b = as_num_array([1,10,50])
  >>> v = as_num_array([0,5,35,100])
  >>> data = as_num_array([100,75,50,25,2,1,0])

  >>> print func(data,boundaries=b)
  [3 3 2 2 1 0 0]

  >>> print func(data,boundaries=b,values=v)
  [100 100  35  35   5   0   0]

  """

  ranking = ('fast','loop')

  tests = (
    Test([0],boundaries=[0])	   			== [0],
    Test([1,2,3,4,5],boundaries=[3])   			== [0,0,0,1,1],
    Test([1,2,1.3,1.4,1.33],boundaries=[1.33])		== [0,1,0,1,0],
    Test([100,75,50,25,2,1,0],boundaries=[1,10,50])	== [3,3,2,2,1,0,0],
    Test([100,75,50,25,2,1,0],boundaries=[1,10,50],values=[0,5,35,100])	== [100,100,35,35,5,0,0],
    Test([100,-25,50,75,2,0,1],boundaries=[1,10,50],values=[0,5,35,100])== [100,0,35,100,5,0,0],
    Test([1,2,3,4,5],boundaries=[3,2,1])		== [0,0,0,3,3], # nonsense
  )

  @staticmethod
  def fast(arg,boundaries=[0,100,1000],values=None):
    assert len(boundaries), "at least one boundary is required"
    if values is not None:
      assert len(boundaries)+1 == len(values), "len(values) must be len(boundaries)+1, (%s,%s)" % (len(values),len(boundaries))
    idx = searchsorted(boundaries,arg)
    if values is None:
      return idx
    return as_num_array(values).take(idx)

  @staticmethod
  def loop(arg,boundaries=[0,100,1000],values=None):
    assert len(boundaries), "at least one boundary is required"
    if values is not None:
      assert len(boundaries)+1 == len(values), "len(values) must be len(boundaries)+1, (%s,%s)" % (len(values),len(boundaries))
    idx = []
    for value in arg:
      for i,x in it.izip(it.count(),boundaries):
        if value <= x:
          idx.append(i)
          break
      else:
        idx.append(len(boundaries))
    if values is None:
      return idx
    return [values[i] for i in idx]


########################################################################
discretize = Discretize()

class LinearBins(EquivUnary):
  """Discretize to given number of equal sized bins, returning either
    1) if idx==True, the bin number (0 to bins-1) for each value.
    2) otherwise returning midpoint value for the range in each bin.
  This is mainly a convenience wrapper to Discretize().

  """

  ranking = ('wrapper',)

  tests = (
    Test([0])	   				== [0],
    Test([0,2],bins=1)   			== [1.0,1.0],
    Test([0,2],bins=2)   			== [0.5,1.5],
    Test([0,2],bins=3)   			** [0.333333,1.666666],
    Test([0,1,2],bins=3)   			** [0.333333,1.0,1.666666],
    Test([1,2,3,4,5],bins=3)   			** [1.666666,1.666666,3.0,4.333333,4.333333],
    Test([1,2,1.3,1.4,1.33],bins=2)		== [1.25,1.75,1.25,1.25,1.25],
    Test([1,2,1.3,1.4,1.33],bins=2,min=0.5)	== [0.875,1.625,1.625,1.625,1.625],
    Test([1,2,1.3,1.4,1.33],bins=2,min=-1)	== [1.25,1.25,1.25,1.25,1.25],

    Test([0],idx=True)	   				== [0],
    Test([0,2],bins=1,idx=True)   			== [0,0],
    Test([0,2],bins=2,idx=True)   			== [0,1],
    Test([0,2],bins=3,idx=True)   			== [0,2],
    Test([0,1,2],bins=3,idx=True)   			== [0,1,2],
    Test([1,2,3,4,5],bins=3,idx=True)   		== [0,0,1,2,2],
    Test([1,2,1.3,1.4,1.33],bins=2,idx=True)		== [0,1,0,0,0],
    Test([1,2,1.3,1.4,1.33],bins=2,min=0.5,idx=True)	== [0,1,1,1,1],
    Test([1,2,1.3,1.4,1.33],bins=2,min=-1,idx=True)	== [1,1,1,1,1],
    Test([100,75,50,25,2,1,0],bins=35,max=500,idx=True)	== [6,5,3,1,0,0,0],
  )

  @staticmethod
  def wrapper(arg,bins=10,min=None,max=None,idx=None):
    arg = as_num_array(arg)
    if min is None:
      min = arg.min()
    if max is None:
      max = arg.max()
    step = (max - min) / bins
    if not step:
      return zeros(len(arg))
    boundaries = arange(min+step,max+step,step)
    if idx:
      values = None
    else:
      values = arange(min+(step/2),max+step+step,step)[:len(boundaries)+1]
    return discretize(arg,boundaries=boundaries,values=values)


########################################################################

class MidpointsInteger(EquivUnary):
  """Return list of midpoints for all unique values in given vector.
    One typical use is to choose possible cutpoints when tree building.
    Note: returned vector has length of len(unique_values)-1.
    Note: this method applies only to integer vectors and returns
          integer vector
  """

  ranking = ('vector','naive')

  tests = (
    Test([1])				== [],
    Test([2])				== [],
    Test([1,1,2])			== [2],
    Test([2,2,3])			== [3],
    Test([1,3,5])			== [2,4],
    Test([-5,-1,0,1,8,2,2])		== [-3,0,1,2,5],
  )

  @staticmethod
  def naive(arg):
    uniq = sorted(set(arg))
    out = []
    for i in range(len(uniq)-1):
      a,b = uniq[i:i+2]
      out.append(((a+b)+1)//2)
    return out

  @staticmethod
  def vector(arg):
    uniq = as_num_array(sorted(set(arg)))
    if len(uniq) <= 1:
      return []
    # assuming integer values and '<' op, the +1 causes round-up
    return (uniq[1:]+uniq[:-1]+1)//2

########################################################################

class MidpointsFloat(EquivUnary):
  """Return list of midpoints for all unique values in given vector.
    One typical use is to choose possible cutpoints when tree building.
    Note: returned vector has length of len(unique_values)-1.
    Note: this method applies to any numeric vector type and returns
          float vector.
  """

  ranking = ('vector','naive')

  tests = (
    Test([1.0])				== [],
    Test([2.0])				== [],
    Test([1,3,5])			== [2.0,4.0],
    Test([1,3,3])			== [2.0],
    Test([1.2,1.3,1.5])			== [1.25,1.4],
  )

  @staticmethod
  def naive(arg):
    uniq = sorted(set(arg))
    out = []
    for i in range(len(uniq)-1):
      a,b = uniq[i:i+2]
      out.append((a+b)/2.0)
    return out

  @staticmethod
  def vector(arg):
    uniq = as_num_array(sorted(set(arg)))
    if len(uniq) <= 1:
      return []
    return (uniq[1:]+uniq[:-1])/2.0


########################################################################

if __name__ == "__main__":
  from base import tester
  tester.testmod()

########################################################################
