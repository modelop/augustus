"""ChangeDetectionModel

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
from base import corelib, Test, EquivUnary, EquivBinary, as_num_array
log = corelib.log

########################################################################

class ChangeDetect(EquivBinary):
  '''Change Detection Model

  '''
  name = 'change_detect'
  ranking = ('fast',)

  @staticmethod
  def fast(arg1,arg2,reset_value=0.0,out=None):
    arg1 = as_num_array(arg1)
    arg2 = as_num_array(arg2)
    if not out:
      out = arg1.new()
    cusum_func = CusumReset().iterfunc
    log_odds = log(arg2/arg1)
    cusum_func(log_odds,reset_value=reset_value,out=out)
    return out
    

########################################################################

class CusumReset(EquivUnary):
  '''CUSUM with reset algorithm

    >>> func = CusumReset().iterfunc
    >>> assert func([1,-3,6,7,-7,-9]).tolist() == [1,0,6,13,6,0]

  '''
  name = 'cusum_reset'
  ranking = ('iterfunc',)
  #ranking = ('iterfunc','iterloop')

  tests = (
    Test([1,-3,6,7,-7,-9]) == [1,0,6,13,6,0],
  )

  @staticmethod
  def iterfunc(arg,reset_value=0.0,out=None):
    def gen_cusum(data,reset_value=0.0):
      # no obvious way to vectorize this
      out = 0.0
      for value in data:
        out = max(reset_value,out+value)
        yield out

    arg = as_num_array(arg)
    if out is None:
      out = arg.new()
    out[:] = list(gen_cusum(arg,reset_value))
    return out


  @staticmethod
  def iterloop(arg,reset_value=0.0,out=None):
    arg = as_num_array(arg)
    if not out:
      out = arg.new()
    last = 0.0
    for i,value in it.izip(it.count(),arg):
      out[i] = max(reset_value,last+value)
    return out
    

########################################################################

if __name__ == "__main__":
  from base import tester
  tester.testmod()

########################################################################
