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
