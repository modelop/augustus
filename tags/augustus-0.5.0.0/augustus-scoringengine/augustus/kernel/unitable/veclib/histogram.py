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

"""Histogram extractions

"""

import itertools as it
groupby = it.groupby
from base import corelib, EquivUnary, Test, as_any_array

########################################################################

class HistoTuple(EquivUnary):
  '''Histogram returned as list of (value,count) tuples

  '''
  name = 'histo_tuple'
  ranking = ('iter_groupby','dict1')

  tests = (
    Test([1,2,3,4,1,2,3,1,1,1,9]) ==  [(1,5),(2,2),(3,2),(4,1),(9,1)],
    Test([1,1,1,1,1,1,1,1,1,1,1]) ==  [(1,11)],
  )

  @staticmethod
  def iter_groupby(arg):
    arg = as_any_array(arg)
    return sorted([(k,len(list(g))) for k,g in groupby(sorted(arg))])

  @staticmethod
  def dict1(arg,out=None):
    arg = as_any_array(arg)
    out = {}
    for val in arg:
        out[val] = out.get(val,0) + 1
    return sorted(out.items())

########################################################################

if __name__ == "__main__":
  from base import tester
  tester.testmod()

########################################################################
