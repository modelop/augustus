"""Application of functions to sliding windows within data

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
izip = it.izip
chain = it.chain
from numpy import (arange, average, minimum, maximum,
  ones, zeros,
  BooleanType, nonzero, concatenate)
from base import (corelib, EquivUnary, EquivBinary, Test,
  as_any_array, as_num_array)

########################################################################

def arg_sel_step_to_idx(arg,sel=None,step=1):
  assert step >= 1
  if sel is None:
    idx = arange(len(arg))
  else:
    sel = as_num_array(sel)
    if isinstance(sel.type(),BooleanType):
      idx = nonzero(sel)[0]
    else:
      idx = sel.astype('Int')
  idx = concatenate(([0],idx))
  return idx

########################################################################

class WindowApply(EquivUnary):
  """Apply function to sliding window within single data vector

  """
  name = 'window_apply'
  ranking = ('naive1','naive2')
  bench_sizes = (1,2,3,4,5,10,20,50,100,200,300,500,1000,5000)

  tests = (
    Test(arange(20),sel=[3,5,9,19])	== [6,9,30,145],

    Test(arange(10),sel=zeros(10,type='Bool'),step=1)	== [],
    Test(arange(10),sel=ones(10,type='Bool'),step=1)	\
    	== [0,1,2,3,4,5,6,7,8,9],
    Test(arange(10),sel=(arange(10)%2)==0,step=1)	== [0,3,7,11,15],
    Test(arange(10),sel=(arange(10)%2)!=0,step=1)	== [1,5,9,13,17],

    Test(arange(10),step=2)		== [1,3,5,7,9,11,13,15,17],
    Test(arange(1,11),step=2)		== [2,5,7,9,11,13,15,17,19],
    Test(arange(10),step=3)		== [3,6,9,12,15,18,21,24],
    Test(arange(1,11),step=3)		== [5,9,12,15,18,21,24,27],
    Test(arange(2,12),step=3)		== [7,12,15,18,21,24,27,30],
    Test([0,1,2,3],func=sum)		== [0,1,2,3],
    Test([0,1,2,3],step=2)		== [1,3,5],
    #Test([0,1,2,3],func=minimum)	== [0,1,2,3],
  )

  def _prep_testdata_broken(self,*args,**kwargs):
    out = [as_num_array(arg) for arg in args]
    if not kwargs:
      # automatic test cases
      kwargs['sel'] = as_num_array(args[0]) == 0
    return (out,kwargs)

  @staticmethod
  def naive1(arg,sel=None,step=1,func=sum):
    arg = as_num_array(arg)
    idx = arg_sel_step_to_idx(arg,sel,step)
    out = []
    for i in xrange(len(idx)-step):
      j = idx[i]
      k = idx[i+step]
      chunk = arg[j+1:k+1]
      out.append(func(chunk))
    return as_num_array(out)

  @staticmethod
  def naive2(arg,sel=None,step=1,func=sum):
    arg = as_num_array(arg)
    idx = arg_sel_step_to_idx(arg,sel,step)
    jj = idx[:-step]
    kk = idx[step:]
    out = []
    for j,k in izip(jj,kk):
      chunk = arg[j+1:k+1]
      out.append(func(chunk))
    return as_num_array(out)

########################################################################

class WindowRange(EquivUnary):
  """Get value range within sliding window on single data vector

  """
  name = 'window_range'
  ranking = ('naive1','naive2')
  ranking = ('naive1',)
  bench_sizes = (1,2,3,4,5,10,20,50,100,200,300,500,1000,5000)

  tests = (
    Test([0,1,2,3])			== [0,0,0],
    Test([0,1,2,3],step=2)		== [0,1,1],
    Test(arange(20),sel=[0,3,5,9,20])	== [2,1,3,9],
  )

  @staticmethod
  def naive1(arg,sel=None,step=1):
    arg = as_num_array(arg)
    idx = arg_sel_step_to_idx(arg,sel,step)
    jj = idx[:-step]
    kk = idx[step:]
    segments = [arg[j+1:k+1] for j,k in izip(jj,kk) if j!=k]
    omin = [seg.min() for seg in segments if len(seg)]
    omax = [seg.max() for seg in segments if len(seg)]
    return as_num_array(omax) - omin

  def naive2(arg,sel=None,step=1):
    arg = as_num_array(arg)
    idx = arg_sel_step_to_idx(arg,sel,step)
    jj = idx[:-step]
    kk = idx[step:]
    omin = [arg[j+1:k+1].min() for j,k in izip(jj,kk)]
    omax = [arg[j+1:k+1].max() for j,k in izip(jj,kk)]
    return as_num_array(omax) - omin

########################################################################

if __name__ == "__main__":
  from base import tester
  tester.testmod()

########################################################################
