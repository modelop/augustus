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

############################################################################
############################################################################

class BaseVectorMethods(object):
  '''Collection of basic vector methods

  These methods assume that the input arg(s) are already numpy arrays.
  In a "universal" context, they should be wrapped to ensure the input type.

  There are often multiple obvious ways to implement these methods.
  Care has been taken to ensure highest performance for large arrays.
  '''

  @staticmethod
  def delta(arg):
    '''difference from previous element

    >>> delta = BaseVectorMethods().delta
    >>> a = na.asarray([3,6,8,9])
    >>> delta(a)
    array([0, 3, 2, 1])
    >>> b = na.asarray([3,6,2,1])
    >>> delta(b)
    array([ 0,  3, -4, -1])
    '''
    return na.concatenate(([0],arg[1:]-arg[:-1]))
    

  @staticmethod
  def delta0(arg):
    '''difference from previous element

    >>> delta = BaseVectorMethods().delta
    >>> a = na.asarray([3,6,8,9])
    >>> delta(a)
    array([0, 3, 2, 1])
    >>> b = na.asarray([3,6,2,1])
    >>> delta(b)
    array([ 0,  3, -4, -1])
    '''
    out = arg.copy()
    out[0] = 0
    out[1:] -= arg[:-1]
    return out

  @staticmethod
  def carry(arg):
    '''carry forward previous non-zero value

    >>> carry = BaseVectorMethods().carry
    >>> a = na.asarray([0,6,0,0,1,2,0,0,9])
    >>> carry(a)
    array([0, 6, 6, 6, 1, 2, 2, 2, 9])
    '''
    out = arg.copy()
    idx = index_nonzero(out)
    try:
      first = idx[0]
    except:
      first = len(out)
    out[:first] = 0
    idx2 = list(idx[1:])+[len(out)]
    for start,stop in zip(idx,idx2):
      out[start:stop] = out[start]
    return out


############################################################################
# local helpers

def index_nonzero(arg): return na.nonzero(arg)[0]

############################################################################

if __name__ == "__main__":
  import doctest
  doctest.testmod()

############################################################################
