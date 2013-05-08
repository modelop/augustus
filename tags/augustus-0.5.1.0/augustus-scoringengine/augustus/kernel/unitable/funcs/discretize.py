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

import numpy as na
asarray = na.asarray

class Discretize(object):
  """data binning function

  >>> b = [1,10,50]
  >>> v = [0,5,35,100]
  >>> data = [100,75,50,25,2,1,0]

  >>> disc = Discretize(b)
  >>> print disc(data)
  [3 3 2 2 1 0 0]

  >>> disc = Discretize(b,v)
  >>> print disc(data)
  [100 100  35  35   5   0   0]

  """
  def __init__(self,boundaries,values=None):
    assert len(boundaries), "at least one boundary is required"
    if values is None:
      self.boundaries = na.sort(boundaries)
      self.values = values
    else:
      assert len(boundaries)+1 == len(values), "len(values) must be len(boundaries)+1"
      lastvalue = values[-1]
      boundvalues = sorted(zip(boundaries,values))
      self.boundaries = asarray([x[0] for x in boundvalues])
      self.values = asarray([x[1] for x in boundvalues]+[lastvalue])

  def __call__(self,data):
    idx = na.searchsorted(self.boundaries,data)
    if self.values is None:
      return idx
    return self.values.take(idx)

#################################################################

if __name__ == "__main__":
  import doctest
  doctest.testmod()

#################################################################
# vim:sw=2:sts=2:expandtab:shiftround
