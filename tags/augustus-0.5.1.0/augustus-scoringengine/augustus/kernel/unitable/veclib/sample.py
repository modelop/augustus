#!/usr/bin/env python

# Copyright (C) 2006-2011  Open Data ("Open Data" refers to
# one or more of the following companies: Open Data Partners LLC,
# Open Data Research LLC, or Open Data Capital LLC.)
#
# This file is part of Augustus.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


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
