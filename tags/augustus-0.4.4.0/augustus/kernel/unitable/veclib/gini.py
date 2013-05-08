"""GINI

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
groupby = it.groupby
from numpy import sort, concatenate
from base import corelib, EquivUnary, EquivBinary, Test, as_any_array, as_num_array

########################################################################

class Gini(EquivUnary):
  """GINI on a single vector

  """
  itypes = 'i'

  name = 'gini'
  ranking = ('groupby2','smart','loop3','groupby3','groupby1','loop2','loop1')

  tests = (
    Test([0,0,0])			== 0.0,
    Test([1,1,1])			== 0.0,
    Test([0,0,1])			** 0.444444444444,
    Test([0,1,1])			** 0.444444444444,
    Test([1,1,2])			** 0.444444444444,
    Test([1,2,2])			** 0.444444444444,
    Test([1,2,3])			** 0.666666666667,
    Test([1,2,3,4])			== 0.75,
    Test([4,3,2,1])			== 0.75,
    Test([1,2,3,2,3,3])			** 0.611111111111,
    Test([2,1,3,200,3000,30000])	** 0.833333333333,
    Test(range(100))			** 0.99,
    Test(range(1000))			** 0.999,
  )

  @staticmethod
  def smart(arg):
    arg = as_num_array(arg)
    if len(arg) < 50:
      return Gini.loop3(arg)
    return Gini.groupby2(arg)

  @staticmethod
  def groupby3(arg):
    arg = as_num_array(arg)
    n = float(len(arg))
    gfx = as_num_array([len(list(g)) for k,g in groupby(sort(arg))])/n
    gfx *= gfx
    out = 1.0 - gfx.sum()
    return out

  @staticmethod
  def groupby2(arg):
    arg = as_num_array(arg)
    n = float(len(arg))
    gfx = [len(list(g))/n for k,g in groupby(sorted(arg))]
    out = 1.0
    for gf in gfx:
      out -= gf * gf
    return out

  @staticmethod
  def groupby1(arg):
    arg = as_num_array(arg)
    histo = [(k,len(list(g))) for k,g in groupby(sorted(arg))]
    n = float(len(arg))
    out = 1.0
    for (val,cnt) in histo:
      gf = cnt/n
      out -= gf * gf
    return out

  @staticmethod
  def loop3(arg):
    arg = as_num_array(arg)
    n = float(len(arg))
    enum = {}
    for val in arg:
      enum[val] = 1 + enum.setdefault(val,0)
    out = 1.0
    for cnt in enum.itervalues():
      gf = (cnt*1.0)/n
      out -= gf * gf
    return out

  @staticmethod
  def loop2(arg):
    arg = as_num_array(arg)
    n = float(len(arg))
    enum = {}
    for val in arg:
      cnt = enum.get(val,0)
      enum[val] = cnt + 1
    out = 1.0
    for (val,cnt) in enum.iteritems():
      gf = (cnt*1.0)/n
      out -= gf * gf
    return out

  @staticmethod
  def loop1(arg):
    arg = as_num_array(arg)
    n = float(len(arg))
    enum = {}
    for val in arg:
      cnt = enum.get(val,0)
      enum[val] = cnt + 1
    out = 1.0
    for (val,cnt) in enum.items():
      gf = (cnt*1.0)/n
      out -= gf * gf
    return out

########################################################################

class GiniPresorted(Gini):
  """GINI on a single vector, optimized for presorted data

  """
  name = 'gini_presorted'
  ranking = ('groupby2','loop3','groupby3','groupby1','loop2','loop1')

  def _prep_testdata(self,*args,**kwargs):
    return [sort(as_num_array(arg)) for arg in args]

  @staticmethod
  def groupby3(arg):
    n = float(len(arg))
    gfx = as_num_array([len(list(g)) for k,g in groupby(arg)])/n
    gfx *= gfx
    out = 1.0 - gfx.sum()
    return out

  @staticmethod
  def groupby2(arg):
    n = float(len(arg))
    gfx = [len(list(g))/n for k,g in groupby(arg)]
    out = 1.0
    for gf in gfx:
      out -= gf * gf
    return out

  @staticmethod
  def groupby1(arg):
    histo = [(k,len(list(g))) for k,g in groupby(arg)]
    n = float(len(arg))
    out = 1.0
    for (val,cnt) in histo:
      gf = cnt/n
      out -= gf * gf
    return out

########################################################################

class GiniCounts(Gini):
  """GINI on a list of counts derived from a single vector

  """
  name = 'gini_counts'
  ranking = ('smart','vector1','naive2','naive1')

  def _prep_testdata(self,*args,**kwargs):
    out = []
    for arg in args:
      enum = {}
      for val in arg:
        enum[val] = 1 + enum.setdefault(val,0)
      out.append(as_num_array(enum.values()))
    return out

  @staticmethod
  def smart(arg):
    if len(arg) < 100:
      return GiniCounts.naive1(arg)
    if len(arg) < 1000:
      return GiniCounts.naive2(arg)
    return GiniCounts.vector1(arg)

  @staticmethod
  def vector1(arg):
    n = float(arg.sum())
    gfx = arg/n
    gfx2 = gfx * gfx
    return 1.0 - gfx2.sum()

  @staticmethod
  def naive2(arg):
    n = float(arg.sum())
    gfx = arg/n
    out = 1.0
    for gf in gfx:
      out -= gf * gf
    return out

  @staticmethod
  def naive1(arg):
    n = float(arg.sum())
    out = 1.0
    for cnt in arg:
      gf = cnt/n
      out -= gf * gf
    return out

########################################################################
gini = Gini()

class Gini2(EquivBinary):
  """GINI on a pair of vectors

  """
  itypes = ('i','i')

  name = 'gini2'
  ranking = ('simple2','simple1')

  tests = (
    Test([1,1,1],[1,1,1])		== 0.0,
    Test([1,1,1],[2,2,2])		== 0.0,
    Test([1,1,1],[1,1,2])		** 0.222222222222,
    Test([1,2,3],[1,2,3])		** 0.666666666667,
    Test([1,2,3],[1,2,3,4,5])		** 0.75,
  )

  @staticmethod
  def simple2(arg1,arg2):
    args = [as_num_array(arg) for arg in (arg1,arg2) if len(arg)]
    n = float(sum(len(arg) for arg in args))
    return sum((gini(arg)*len(arg)/n) for arg in args)

  @staticmethod
  def simple1(arg1,arg2):
    gini = Gini()
    args = [as_num_array(arg) for arg in (arg1,arg2) if len(arg)]
    n = float(sum(len(arg) for arg in args))
    return sum((gini(arg)*len(arg)/n) for arg in args)

########################################################################
gini_presorted = GiniPresorted()

class Gini2Presorted(Gini2):
  """GINI on a pair of vectors, optimized for presorted data

  """
  name = 'gini2_presorted'
  ranking = ('simple2','simple1')

  def _prep_testdata(self,*args,**kwargs):
    return [sort(as_num_array(arg)) for arg in args]

  @staticmethod
  def simple2(arg1,arg2):
    args = [arg for arg in (arg1,arg2) if len(arg)]
    n = float(sum(len(arg) for arg in args))
    return sum((gini_presorted(arg)*len(arg)/n) for arg in args)

  @staticmethod
  def simple1(arg1,arg2):
    gini = GiniPresorted()
    args = [arg for arg in (arg1,arg2) if len(arg)]
    n = float(sum(len(arg) for arg in args))
    return sum((gini(arg)*len(arg)/n) for arg in args)

########################################################################
gini_counts = GiniCounts()

class Gini2Counts(Gini2):
  """GINI on a pair of count vectors

  """
  name = 'gini2_counts'
  ranking = ('vector1','simple2','simple1')

  def _prep_testdata(self,*args,**kwargs):
    out = []
    for arg in args:
      enum = {}
      for val in arg:
        enum[val] = 1 + enum.setdefault(val,0)
      out.append(as_num_array(enum.values()))
    return out

  @staticmethod
  def vector1(arg1,arg2):
    a1 = as_num_array(arg1)
    a2 = as_num_array(arg2)
    c1 = a1.sum()
    c2 = a2.sum()
    n = float(c1+c2)
    return (gini_counts(a1)*c1/n) + (gini_counts(a2)*c2/n)

  @staticmethod
  def simple2(arg1,arg2):
    c1 = sum(arg1)
    c2 = sum(arg2)
    n = float(c1+c2)
    return (gini_counts(arg1)*c1/n) + (gini_counts(arg2)*c2/n)

  @staticmethod
  def simple1(arg1,arg2):
    gini = GiniCounts()
    args = [arg1,arg2]
    n = float(sum(sum(arg) for arg in args))
    return sum((gini(arg)*sum(arg)/n) for arg in args)

########################################################################

class GiniGain(EquivBinary):
  """GINI gain for a given split

  Note: this is not likely to be the most efficient method for tree
  building since it would result in repeated calculation of the gini()
  of the combined nodes.  Better to have the application calculate that
  once and then loop over the possible splits.
  
  This method is intended more as documentation and as an additional
  test of the combination of the above methods.

  """
  itypes = ('i','i')

  name = 'gini_gain'
  ranking = ('simple1',)

  tests = (
    Test([],[1,1,1,1,1,1])		== 0.0,
    Test([1],[1,1,1,1,1])		== 0.0,
    Test([1,1,1],[1,1,1])		== 0.0,
    Test([1],[1,1,2,2,2])		** 0.1,
    Test([1,1],[1,2,2,2])		== 0.25,
    Test([1,1,1],[2,2,2])		== 0.5,
    Test([1,1,1,2],[2,2])		== 0.25,
    Test([1,1,1],[1,1,2])		** 0.055555555555,
    Test([1,2,3],[1,2,3])		== 0.0,
    Test([1,2,3],[1,2,3,4,5])		** 0.03125,
    Test([1,3],[2,999]) 		== 0.25,
  )

  @staticmethod
  def simple1(arg1,arg2):
    gini1 = Gini()
    gini2 = Gini2()
    args = [as_num_array(arg) for arg in (arg1,arg2) if len(arg)]
    if len(args) != 2:
      return 0.0
    return gini1(concatenate(args)) - gini2(*args)

########################################################################

if __name__ == "__main__":
  from base import tester
  tester.testmod()

########################################################################
