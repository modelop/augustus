"""Universal Tables based on numpy array storage

  Key order is maintained and may be indexed by name or number::

      >>> data = {'a':(1,2,3),'ts':(34567,35678,34657),'values':(5.4,2.2,9.9)}
      >>> keyorder = ('a','ts','values')
      >>> t = UniTable(keys=keyorder,**data)
      >>> print t
      +-+-----+------+
      |a|  ts |values|
      +-+-----+------+
      |1|34567|   5.4|
      |2|35678|   2.2|
      |3|34657|   9.9|
      +-+-----+------+

      >>> print t[0]
      [1, 34567, 5.4000000000000004]
      >>> print t['ts']
      [34567 35678 34657]
      >>> print t.field(2)
      [ 5.4  2.2  9.9]

  Row Records maintain link to original data::

      >>> rec = t[1]
      >>> print rec
      [2, 35678, 2.2000000000000002]
      >>> rec[2] = 2.3
      >>> rec[1] = 99999
      >>> print rec
      [2, 99999, 2.2999999999999998]
      >>> print repr(rec)
      row 1: {'a':2, 'ts':99999, 'values':2.2999999999999998}
      >>> print t
      +-+-----+------+
      |a|  ts |values|
      +-+-----+------+
      |1|34567|   5.4|
      |2|99999|   2.3|
      |3|34657|   9.9|
      +-+-----+------+

  Additional examples::

      >>> junk = t.newfield('addon')
      >>> t['alpha'] = ['some','text','values']
      >>> print t
      +-+-----+------+-----+------+
      |a|  ts |values|addon|alpha |
      +-+-----+------+-----+------+
      |1|34567|   5.4|    0|  some|
      |2|99999|   2.3|    0|  text|
      |3|34657|   9.9|    0|values|
      +-+-----+------+-----+------+

      >>> t['added'] = t['a'] + t['ts']
      >>> print t
      +-+-----+------+-----+------+------+
      |a|  ts |values|addon|alpha |added |
      +-+-----+------+-----+------+------+
      |1|34567|   5.4|    0|  some| 34568|
      |2|99999|   2.3|    0|  text|100001|
      |3|34657|   9.9|    0|values| 34660|
      +-+-----+------+-----+------+------+

      >>> t.append((4,77777,6.6,1,'here',88888))
      >>> print t
      +-+-----+------+-----+------+------+
      |a|  ts |values|addon|alpha |added |
      +-+-----+------+-----+------+------+
      |1|34567|   5.4|    0|  some| 34568|
      |2|99999|   2.3|    0|  text|100001|
      |3|34657|   9.9|    0|values| 34660|
      |4|77777|   6.6|    1|  here| 88888|
      +-+-----+------+-----+------+------+

      >>> print t[1:3]
      +-+-----+------+-----+------+------+
      |a|  ts |values|addon|alpha |added |
      +-+-----+------+-----+------+------+
      |2|99999|   2.3|    0|  text|100001|
      |3|34657|   9.9|    0|values| 34660|
      +-+-----+------+-----+------+------+

      >>> print t.take([1,3])
      +-+-----+------+-----+-----+------+
      |a|  ts |values|addon|alpha|added |
      +-+-----+------+-----+-----+------+
      |2|99999|   2.3|    0| text|100001|
      |4|77777|   6.6|    1| here| 88888|
      +-+-----+------+-----+-----+------+

      >>> print t.subtbl(t['a']-3 == t['addon'])
      +-+-----+------+-----+------+-----+
      |a|  ts |values|addon|alpha |added|
      +-+-----+------+-----+------+-----+
      |3|34657|   9.9|    0|values|34660|
      |4|77777|   6.6|    1|  here|88888|
      +-+-----+------+-----+------+-----+

      >>> t2 = t.sorted_on('values')
      >>> print t2
      +-+-----+------+-----+------+------+
      |a|  ts |values|addon|alpha |added |
      +-+-----+------+-----+------+------+
      |2|99999|   2.3|    0|  text|100001|
      |1|34567|   5.4|    0|  some| 34568|
      |4|77777|   6.6|    1|  here| 88888|
      |3|34657|   9.9|    0|values| 34660|
      +-+-----+------+-----+------+------+
      >>> assert t2 != t
      >>> t2.sort_on('a')
      >>> assert t2 == t

      >>> print t.to_csv_str(sep=',')
      a,ts,values,addon,alpha,added
      1,34567,5.4,0,some,34568
      2,99999,2.3,0,text,100001
      3,34657,9.9,0,values,34660
      4,77777,6.6,1,here,88888

      >>> for row in t.izip(): print row
      (1, 34567, 5.4000000000000004, 0, 'some', 34568)
      (2, 99999, 2.2999999999999998, 0, 'text', 100001)
      (3, 34657, 9.9000000000000004, 0, 'values', 34660)
      (4, 77777, 6.5999999999999996, 1, 'here', 88888)

      # timing test
      #>>> for n in range(100000): t.append([n]*6)

      # file I/O test
      >>> filename = '/tmp/unitable.dat'
      >>> t.to_nab_file(filename)
      >>> s = t.from_nab_file(filename)
      >>> print t == s
      True
      >>> s.append((5,56789,4.5,1,'and here',87654))
      >>> print s.to_csv_str(sep=':')
      a:ts:values:addon:alpha:added
      1:34567:5.4:0:some:34568
      2:99999:2.3:0:text:100001
      3:34657:9.9:0:values:34660
      4:77777:6.6:1:here:88888
      5:56789:4.5:1:and here:87654

      # operations with no python infix equivalent
      >>> print UniTable.logical_and(t['a'],t['addon'])
      [False False False True True]
      >>> print UniTable.logical_not(t['addon'])
      [ True  True  True False False]

      # controlled error handling
      #>>> t.Error.setMode(dividebyzero="ignore")
      #>>> print t['values']/(t['values']-2.3)
      #[  1.74193548e+000               inf   1.30263158e+000
      #   1.53488372e+000]
      #>>> print t['a']/(t['a']-2)
      #[-1  0  3  2]
      #>>> t.Error.setMode()
      #>>> print t['a']/(t['a']-2)
      #Warning: Encountered divide by zero(s)  in divide
      #[-1  0  3  2]
      #>>> t.Error.setMode(all="raise")
      #>>> print t['a']/(t['a']-2)
      #Traceback (most recent call last):
      #...
      #ZeroDivisionError:  in divide

  .. todo:: eval tests seem to be broken.

  ::

      # eval notation
      #>>> print s.eval('a')
      #[1 2 3 4 5]
      #>>> s['a2'] = s.eval('a+a')
      #>>> print s['a2']
      #[ 2  4  6  8 10]
      #>>> s.eval('ts2 = a + ts')
      #>>> print s
      #+-+-----+------+-----+--------+------+--+------+
      #|a|  ts |values|addon| alpha  |added |a2| ts2  |
      #+-+-----+------+-----+--------+------+--+------+
      #|1|34567|   5.4|    0|    some| 34568| 2| 34568|
      #|2|99999|   2.3|    0|    text|100001| 4|100001|
      #|3|34657|   9.9|    0|  values| 34660| 6| 34660|
      #|4|77777|   6.6|    1|    here| 88888| 8| 77781|
      #|5|56789|   4.5|    1|and here| 87654|10| 56794|
      #+-+-----+------+-----+--------+------+--+------+

      #>>> print s.eval('a+(na.logical_and(addon,a2))')
      #Traceback (most recent call last):
      #...
      #NameError: name 'na' is not defined
      #>>> print s.eval('a+(na.logical_and(addon,a2))',na=na)
      #[1 2 3 5 6]

      >>> print s.summary()
      UniTable summary:
        ------------------------------------------------------------------------
        UniTable format summary: 6 fields, 32 bytes/record
          field  1: int32     'a'
          field  2: int32     'ts'
          field  3: float64   'values'
          field  4: int32     'addon'
          field  5: Char(8)   'alpha'
          field  6: int32     'added'
        ------------------------------------------------------------------------
        UniTable data summary: 5 records, 160 bytes total
          a           = [1 2 3 4 5]
          ts          = [34567 99999 34657 77777 56789]
          values      = [ 5.4  2.3  9.9  6.6  4.5]
          addon       = [0 0 0 1 1]
          alpha       = ['some' 'text' 'values' 'here' 'and here']
          added       = [ 34568 100001  34660  88888  87654]
        ------------------------------------------------------------------------

      >>> s.pop('added')
      array([ 34568, 100001,  34660,  88888,  87654])
      >>> s.pop('added')
      >>> s.pop(2)
      array([ 5.4,  2.3,  9.9,  6.6,  4.5])

      >>> ss = s.concatenate(s)
      >>> assert s.extend(s) == ss

  .. todo:: why is this failing?
  
  ::
  
      #>>> print s.get_type_codes()
      #['int32', 'int32', 'float64', 'int32', 'a8', 'int32']
      #>>> print s.compact().get_type_codes()
      #['int8', 'int32', 'bool', 'a8', 'int8', 'int32']

      >>> e = UniTable(keys=['a','b','c'])
      >>> e.append((1,2,'xx'))
      >>> print e
      +-+-+--+
      |a|b|c |
      +-+-+--+
      |1|2|xx|
      +-+-+--+
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

import os
import sys
import operator
import numpy as na
from numpy import char as nastr
import itertools as it

from asarray import asarray, compact, get_format, any_compress
from storage import TableMethods

# for convenience

logical_and = na.logical_and
logical_or  = na.logical_or
logical_xor = na.logical_xor
logical_not = na.logical_not

########################################################################

class UniDict(object):
  """mixin class providing dict-like interface with universal indexing

  key order is preserved and key values may be strings or numbers, in
  which case the number is used to lookup the string name.
  """
  def __init__(self,keys=None):
    self._keys = []
    self._data = {}
    if keys is not None:
      self.setkeys(keys)

  def __len__(self):
    '''length, not number, of columns'''
    if not len(self._data):
      return 0
    return max(len(x) for x in self.values())

  def width(self): return len(self._data)

  def _mapkey(self,key,add_missing=False):
    if isinstance(key,int):
      try:
        key = self._keys[key]
      except IndexError:
        if add_missing:
          raise KeyError, 'numeric key not allowed when adding new entry'
        else:
          raise KeyError, 'numeric key out of range'
    if not isinstance(key,basestring):
      raise KeyError, 'key must be numeric or string for this operation'
    if key not in self._keys:
      if add_missing:
        self._keys.append(key)
        self._data[key] = None
    return key

  def __getitem__(self,anykey): return self._gethook(self._mapkey(anykey))
  def __setitem__(self,anykey,value): self._sethook(self._mapkey(anykey,True),value)
  def _gethook(self,key): return self._data[key]
  def _sethook(self,key,value): self._data[key] = value

  def setkeys(self,keys):
    for key in keys:
      self._mapkey(key,True)

  def has_key(self,anykey):
    if isinstance(anykey,int):
      return 0 <= anykey < len(self._keys)
    else:
      return self._data.has_key(anykey)

  def keys(self): return self._keys[:]
  def values(self): return [self[key] for key in self._keys]
  def items(self): return [(key,self[key]) for key in self._keys]

  def setdefault(self,anykey,failobj=None):
    if not anykey in self:
      self[anykey] = failobj
    return self[anykey]

  def update(self,other):
    for key,value in other.items():
      self[key] = value

  def __contains__(self,key):
    return self.has_key(key)

  def __iter__(self):
    for k in self.keys():
      yield k

  def __repr__(self):
    out = ['Dict']
    for name in self._keys:
      value = self._data[name]
      out.append('%s: %s' % (name,str(value)))
    return '\n  '.join(out)

  @staticmethod
  def _compile(arg):
    '''compile an expression into a code object ready for eval or exec'''
    if not isinstance(arg,basestring):
      return arg
    import compiler
    desc = 'eval(%r)' % arg
    try:
      # eval mode works for expressions, not assignments
      mode = 'eval'
      code = compiler.compile(arg,desc,mode)
    except SyntaxError:
      # exec mode necessary for assignments
      mode = 'exec'
      code = compiler.compile(arg,desc,mode)
    return code

  def eval(self,code,**context):
    '''evaluate an expression in the local dictionary namespace'''
    code = self._compile(code)
    return eval(code,context,self.as_mapping())

  def as_mapping(self):
    '''return self in minimal mapping wrapper suitable for eval()'''
    class Mapping(object):
      def __init__(self,realself): self._realself = realself
      def __getitem__(self,key): return self._realself[key]
      def __setitem__(self,key,value): self._realself[key] = value
    return Mapping(self)

  def as_record(self):
    '''return self in minimal attribute access wrapper'''
    class Record(object):
      def __init__(self,realself): self._realself = realself
      def __getattr__(self,key): return self._realself[key]
    return Record(self)

  def as_r(self):
    '''return self as dict suitable for use in R'''
    return self._data.copy()

####################################

class UniDictDel(UniDict):
  """UniDict class with item delete hooks"""

  def __delitem__(self,anykey): self._delhook(self._mapkey(anykey))

  def _delhook(self,key):
    self._keys.remove(key)
    del self._data[key]
    
########################################################################

class UniRecord(UniDict):
  """represents a row of a table

  keeps track of the position of the row in the original array
  so that update can be applied.

  note that a UniRecord is it's own iterator.  if it is to be saved
  while iterating it is necessary to copy() it.
  """
  def __init__(self,keys,valuearrays,rownum,step=1,stop=None):
    UniDict.__init__(self)
    self._rownum = rownum
    self._step = step	# support reverse iteration
    if stop is None:
      if step < 0:
        stop = -1
      else:
        try:
          stop = len(valuearrays[0])
        except:
          # assume either no columns or no data
          stop = rownum + step
    self._stop = stop
    if step < 0:
      self._stoper = operator.lt
    else:
      self._stoper = operator.ge
    for (key,valuearray) in zip(keys,valuearrays):
      self._keys.append(key)
      self._data[key] = valuearray

  # iteration interface

  def __iter__(self):
    return self

  def next(self):
    self._rownum += self._step
    if self._stoper(self._rownum,self._stop):
      raise StopIteration
    return self

  # numpy.records Record public interface

  def field(self,anykey): return self[anykey]
  def setfield(self,anykey,value): self[anykey] = value

  # public interface extensions

  def fields(self,*anykeys):
    if not anykeys:
      anykeys = self._keys
    return [self.field(anykey) for anykey in anykeys]

  # customize hooks

  def _gethook(self,key):
    return self._data[key][self._rownum]

  def _sethook(self,key,value):
    self._data[key][self._rownum] = value

  # internal

  def __str__(self):
    out = [self.field(key) for key in self._keys]
    return repr(out)

  def __repr__(self):
    out = ['%r:%r' % (key,val) for key,val in self.items()]
    return 'row %s: {%s}' % (self._rownum,', '.join(out))


########################################################################

class UniTable(UniDictDel,TableMethods):
  """container for named numpy arrays

  the interface is similar to numpy.records but designed to allow
  adding rows and columns efficiently.  the member arrays are stored
  as individual entities, and they should not be resized directly.

  rows (similar to the numpy.records.Record) are returned as
  UniRecord objects

  keys specified in __init__ establish key presentation order.
  """
  def __init__(self,keys=None,_asarray=None,_size=None,_prealloc=0,**kwargs):
    UniDict.__init__(self,keys=keys)
    if _asarray is None:
      _asarray = asarray
    self._asarray = _asarray    # allow easy override of factory func
    self._len = _size           # all arrays must be the same length
    self._prealloc = _prealloc  # memory overallocation for growth
    self.update(kwargs)
    self._postinit_hook()

  def load(self,arg):
    '''if arg is type string, try to load a file of that name.
      Otherwise, assume that it is a mapping (possibly another
      UniTable) and update self from that.
    '''
    if isinstance(arg,basestring):
      self.fromfile(arg)
    elif arg is not None:
      self.update(arg)
    self._postinit_hook()
    return self

  # numpy.records 'Record array' public interface

  def field(self,anykey):
    '''return a single field (column) from this table'''
    return self._gethook(self._mapkey(anykey))

  # public interface extensions

  def fields(self,*anykeys):
    '''return a list of fields (columns) from this table'''
    if not anykeys:
      anykeys = self._keys
    return [self.field(anykey) for anykey in anykeys]

  def __len__(self):
    '''length, not number, of columns'''
    return self._len or 0

  # customized hooks

  def _gethook(self,key):
    return self._get_vector(key)

  def _sethook(self,key,value):
    try:
      len(value)
    except TypeError:
      # scalar value, make it sequence
      value = [value] * (self._len or 1)
    except ValueError:
      # Rank-0 array
      value.resize(self._len or 1)
    else:
      if isinstance(value,basestring):
        value = [value]
    if self._len is None:
      self._len = len(value)
    else:
      assert len(value) == self._len, 'error setting key %r: expected len=%s, got len=%s' \
                                      % (key,self._len,len(value))
    self._data[key] = self._set_vector(key,value)

  def __getitem__(self,anykey):
    '''this is the exception to the interface.
    a numeric getitem does not return a column as
    when using field(). instead, it returns a Record
    object for the given row number.
    '''
    if not isinstance(anykey,int):
      return self.field(anykey)
    if anykey < 0:
      anykey = self._len + anykey
    if not (0 <= anykey < self._len):
      raise IndexError, anykey
    return UniRecord(self._keys,self.values(),anykey)

  def __iter__(self):
    '''iteration takes place over all Records in the table
    (unlike a python dict, which iterates over the keys)'''
    return UniRecord(self._keys,self.values(),-1)

  def _delhook(self,key):
    try:
      self._keys.remove(key)
    except ValueError:
      pass
    self._del_vector(key)

  # customization hooks

  def _get_vector(self,key):
    return self._data[key]

  def _set_vector(self,key,value):
    return self._asarray(value)

  def _del_vector(self,key):
    del self._data[key]

  def _new_hook(self):
    '''return empty instance of self'''
    return UniTable(keys=self._keys,_asarray=self._asarray)

  def _postinit_hook(self):
    '''called after __init__(), load(), and after creation by _new_hook()'''
    pass

  def _grow_hook(self):
    '''return reference to self prepared to grow values'''
    return self

  # enhanced methods

  def eval(self,code,**context):
    '''evaluate an expression in the local table namespace'''
    code = self._compile(code)
    if not context.has_key('_'):
      from veclib import veclib
      context['_'] = veclib
    return eval(code,context,self.as_mapping())

  def reverse_iter(self):
    '''reverse iteration over all Records in the table'''
    return UniRecord(self._keys,self.values(),len(self),-1)

  def izip(self,*keys):
    '''fast iteration over table records as tuples'''
    return it.izip(*self.fields(*keys))

  def pop(self,anykey,default=None):
    '''return specified field, removing from table'''
    key = self._mapkey(anykey)
    try:
      self._keys.remove(key)
    except ValueError:
      pass
    return self._data.pop(key,default)

  def __getslice__(self,i,j):
    '''return new table containing only rows in range [i:j]
    (note that the data is not copied and that changes to
    subtbl values will propagate to the original table)'''
    out = self._new_hook()
    for key in self._keys:
      out[key] = self._data[key][i:j]
    out._postinit_hook()
    return out

  def compact(self,*keys):
    '''adjust all field types to smallest type needed to hold current value range'''
    if not keys:
      keys = self._keys
    for key in keys:
      oval = self[key]
      ''' skip Nones for the compact step'''
      if oval is not None:
          nval = compact(oval,also_floats=True)
          if nval is not oval:
              self[key] = nval
    return self

  def compact_ints(self,*keys):
    '''adjust integer field types to smallest type needed to hold current value range'''
    if not keys:
      keys = self._keys
    for key in keys:
      oval = self[key]
      nval = compact(oval)
      if nval is not oval:
        self[key] = nval
    return self

  def copy(self,*keys):
    '''return copy of this table, optionally with subset of fields'''
    out = self._new_hook()
    if not keys:
      keys = self._keys
    else:
      out._keys = list(keys)
    for key in keys:
      out[key] = self.field(key).copy()
    out._postinit_hook()
    return out

  def view(self,*keys):
    '''return view of this table, optionally with subset of fields'''
    out = self._new_hook()
    if not keys:
      keys = self._keys
    else:
      out._keys = list(keys)
    for key in keys:
      out[key] = self.field(key)
    out._postinit_hook()
    return out

  def rename(self,keymap):
    '''rename fields in-place, preserving original order, ignoring missing'''
    try:
      keymap = keymap.items()
    except AttributeError:
      pass
    for (oldname,newname) in keymap:
      if newname in self._keys:
        raise KeyError, 'new name already exists in table: %r' % newname
      try:
        i = self._keys.index(oldname)
      except ValueError:
        continue
      self._keys[i] = newname
      try:
        self._data[newname] = self._data[oldname]
        del self._data[oldname]
      except KeyError:
        pass

  def subtbl_groupby(self,key):
    '''iterate over subtbl slices based on consecutive equal values
    of a key field (while this is generally applied to a table
    already sorted on key field, it can also be used to process
    consecutive runs in an unsorted table).'''
    j = 0
    for k,g in it.groupby(self[key]):
      i = j
      j = i + len(list(g))
      yield k,self[i:j]

  def subtbl(self,arg):
    '''invoke take() or compress() according to arg type'''
    try:
      ismask = arg.dtype == na.bool_
    except:
      ismask = False
    if ismask:
      return self.compress(arg)
    return self.take(arg)

  def take(self,indices):
    '''return new table containing only rows selected by indices'''
    out = self._new_hook()
    for key in self._keys:
      out[key] = na.take(self._data[key],indices)
    out._postinit_hook()
    return out

  def compress(self,mask):
    '''return new table containing only rows where mask is nonzero'''
    out = self._new_hook()
    for key in self._keys:
      out[key] = any_compress(mask,self._data[key])
    out._postinit_hook()
    return out

  def sorted_on(self,anykey,reverse=False):
    '''return sorted copy of table'''
    sortidx = self.field(anykey).argsort()
    if reverse:
      sortidx = sortidx[::-1]
    return self.take(sortidx)

  def sort_on(self,anykey,reverse=False):
    '''sort table in-place'''
    sortidx = self.field(anykey).argsort()
    if reverse:
      sortidx = sortidx[::-1]
    for key in self._keys:
      self._data[key] = na.take(self._data[key],sortidx)

  def reverse(self,*keys):
    '''reverse table in-place, optionally applying to subset of keys'''
    if not keys:
      keys = self._keys
    if not keys or not self._len:
      return
    for key in self._keys:
      self._data[key] = self._data[key][::-1]

  def __eq__(self,other):
    if not isinstance(other,UniTable):
      return self._data == other
    for key in self._keys:
      if not (na.alltrue(self[key] == other[key])):
        return False
    return True

  def newfield(self,name,default=0,type=None):
    '''add a new field to the table'''
    try:
      value = self._asarray([default],type=type)
      value = value.repeat(self._len)
      self[name] = value
      return value
    except TypeError:
      value = [default]*self._len
      self[name] = value
      return self[name]

  def newalloc(self,name,like,type=None):
    '''new uninitialized field based on characteristics of like field'''
    if isinstance(like,basestring):
      like = self._data[like]
    try:
      value = like.new(type=type)
    except AttributeError:
      # assume char-array
      value = nastr.array(' ',itemsize=like.itemsize(),shape=like.shape)
    self[name] = value
    self._clone_field_hook(src=like,dst=name)
    return value

  def _clone_field_hook(self,src,dst): pass

  # growth of data

  def tune(self,prealloc):
    '''set tuneable parameters, currently just prealloc size'''
    self._prealloc = prealloc

  def resize(self,newsize):
    '''grow or shrink length of all columns in table'''
    if self._len is None or self._len == newsize:
      return
    if self._len > newsize:
      # shrinking leaves buffer as-is and takes view
      self._len = newsize
      for key,value in self.items():
        if value is not None:
          self[key] = value[:newsize]
    elif not self._prealloc:
      # without preallocation, use standard method
      #for value in self.values():
      self._len = newsize
      for key in self._keys:
        value = self[key]
        if value is not None:
          value = na.resize(value, newsize)
          self[key] = value
    else:
      # growing first checks for available buffer space
      need = newsize - self._len
      self._reserve_buffer(need)
      for key,value in self.items():
        if value is None:
          continue
        #print '=====',key,value._shape,(newsize,)
        value._shape = (newsize,)

  def _reserve_buffer(self,lowwater=None,highwater=None):
    '''ensure preallocated buffer space'''
    if self._len is None:
      return
    if highwater is None:
      highwater = self._prealloc
    if lowwater is None:
      lowwater = self._prealloc
    else:
      highwater += lowwater
    for key,value in self.items():
      if value is None:
        continue
      itemsize = value.itemsize()
      buf = value._data
      bufsize = len(buf) / itemsize
      refcnt = sys.getrefcount(buf)
      if value.is_c_array() and bufsize >= self._len + lowwater:
        # existing buffer space is acceptable
        if refcnt <= 3:
          # safety check OK, no other users of buffer
          continue
      goal = self._len + highwater
      value.resize(goal)
      value._shape = (self._len,)

  def append(self,data):
    '''append single list or dict of values as a new row to fields'''
    self._grow_data(data,is_scalar=True)

  def extend(self,*others):
    '''extend fields with values from other UniTable(s)'''
    if not len(self):
      for other in others:
        self.setkeys(other.keys())
    if not self._prealloc:
      return self._concatenate(self._grow_hook(),self._keys,self,*others)
    for other in others:
      self._grow_data(other,is_scalar=False)

  def concatenate(self,*others):
    '''return new Unitable with values concatenated from this+other UniTable(s)'''
    out = self._concatenate(self._new_hook(),self._keys,self,*others)
    out._postinit_hook()
    return out

  def _grow_data(self,data,is_scalar):
    '''append single list or dict of values as a new row to fields'''
    self._grow_hook()
    if isinstance(data,(dict,UniRecord)):
      data = [data[key] for key in self._keys]
    elif isinstance(data,UniDict):
      data = [data[key][0] for key in self._keys]
    if len(data) != len(self._keys):
      raise ValueError, 'wrong width: expecting %s, got %s' % (len(self._keys),len(data))
    if self._len is None:
      # handle an initial data assignment
      # note that this works for both scalar and vector values
      for i,value in enumerate(data):
        self[i] = value
      return
    for key,newval in zip(self._keys,data):
      col = self._data[key]
      # array is numeric, disallow silent char as string interpretation  
      if not col.dtype.char == 'S' and isinstance(newval,basestring):
        raise TypeError, 'cannot append string value to numeric array, key=%s' % key
    # do the append, handling both scalar and vector values
    if is_scalar:
      self._len = self._len + 1
      for key,newval in zip(self._keys,data):
        col = self._data[key]
        col = na.append( col, newval )
        self._data[key] = col
        col = self._data[key]
    else:
      # TODO - see if this needs fixing:
      delta = len(data[0])
      olen = len(self)
      self.resize(self._len+delta)
      #print '==========',olen,len(self),delta
      for key,newval in zip(self._keys,data):
        col = self._data[key]
        #print '=====',len(col),len(newval)
        col[-delta:] = newval

  def _concatenate(self,dest,keys,*sources):
    '''assign to dest: values concatenated from list of UniTable(s)'''
    sources = [source for source in sources if len(source)]
    if not len(sources):
      # all sources are empty
      return dest
    dest._len = None
    for key in keys:
      args = [source[key] for source in sources]
      nonchar = [i for (i,arg) in enumerate(args) if not hasattr(arg,'resized')]
      if len(args) != len(nonchar):
        # at least one arg is Char type, ensure all are same type, size
        for i in nonchar:
          #arg = nastr.num2char(args[i],'%s')
          arg = nastr.asarray([str(arg) for arg in list(args[i])])
          args[i] = arg.truncated()
        sizes = [arg.itemsize() for arg in args]
        maxsize = max(sizes)
        if maxsize != min(sizes):
          # handle resizing Char array
          for i,arg in [(i,arg) for (i,arg) in enumerate(args) if arg.itemsize() != maxsize]:
            args[i] = arg.resized(maxsize)
      dest[key] = na.concatenate(args)
    return dest

  # table comparisons

  def diffkeys(self,other):
    '''compare tables returning keys where contents differ'''
    out = []
    for key in self._keys:
      try:
        if not (na.alltrue(self[key] == other[key])):
          out.append(key)
      except KeyError:
          pass
    return out

  def diffkeys_explore(self,key1,key2,dump_sample=0):
    '''return list of difference observations comparing two fields'''
    out = []
    vals = self.fields(key1,key2)
    notequal = vals[0] != vals[1]
    diffcnt = notequal.sum()
    if diffcnt == 0:
      return out
    tcodes = [get_format(val) for val in vals]
    tchars = [tcode[0] for tcode in tcodes]
    # are all values within floating point fuzz?
    if not tchars.count('a') and na.allclose(*vals):
      out.append('no differences using allclose()')
    # real differences exist
    out.append('%s values differ (%1.2f%% of %s)' % (
      diffcnt,100.0*diffcnt/len(self),len(self)))
    if tcodes[0] != tcodes[1]:
      out.append('field types differ %s' % str(tuple(tcodes)))
    # skip detail if any field is alpha type
    if tchars.count('a'):
      return out
    # differences as different types?
    for typestr in ('Int','Bool'):
      tvals = [v.astype(typestr) for v in vals]
      if na.allclose(*tvals):
        out.append('field values match as type(%r)' % typestr)
    # extract differences and examine in greater detail
    dvals = [any_compress(notequal,val) for val in vals]
    nzmask = [(dval != 0) for dval in dvals]
    if (nzmask[0] != nzmask[1]).sum() == 0:
      # all zeros match, compare the nonzero values
      nzvals = [any_compress(nzmask[0],dval) for dval in dvals]
      ratio = nzvals[1].astype('Float') / nzvals[0].astype('Float')
      factor = ratio.mean()
      if na.allclose(ratio,factor):
        out.append('field values differ by constant factor: %f' % factor)
    delta = dvals[0] - dvals[1]
    dmin,dmax = delta.min(),delta.max()
    out.append('difference mean=%f range=%f (%s to %s)' % (
        delta.mean(),dmax-dmin,dmin,dmax))
    if dump_sample:
      tmp = UniTable()
      tmp['_idx_'] = diffidx = na.nonzero(notequal)[0]
      for key in (key1,key2):
        tmp[key] = self.field(key)[notequal]
      if len(tmp) > dump_sample:
        tmp.resize(dump_sample)
      out.extend(str(tmp).split('\n'))
    return out


  def diff(self,other,label2='table2',label1='table1',dump_sample=0):
    '''return difference report for two tables'''
    outsep = '\n'
    out = []
    tbls = [self,other]
    labels = (label1,label2)
    keys = [set(tbl.keys()) for tbl in tbls]
    # do key sets match?
    for a,b in ((0,1),(1,0)):
      ka,kb,label = keys[a],keys[b],labels[a]
      xkeys = ka.difference(kb)
      if xkeys:
        out.append('%s fields only in %s: %s' % (
          len(xkeys),label,tuple(sorted(xkeys))))
    # can contents be compared?
    sizes = [len(tbl) for tbl in tbls]
    cmpsize = min(*sizes)
    cmpkeys = keys[0].intersection(keys[1])
    if not cmpkeys:
      # no fields in common
      return outsep.join(out)
    if sizes[0] != sizes[1]:
      out.append('sizes differ: len(%s)=%s len(%s)=%s' % (
        labels[0],sizes[0],labels[1],sizes[1]))
      if not cmpsize:
        # one table is empty, skip the rest
        return outsep.join(out)
      # adjust sizes to enable partial comparison
      for i in (0,1):
        if sizes[i] > cmpsize:
          out.append('truncating %s for comparison' % labels[i])
          tmptbl = tbls[i].copy(*list(cmpkeys))
          tmptbl.resize(cmpsize)
          tbls[i] = tmptbl
    elif not cmpsize:
      # both tables are empty
      return outsep.join(out)
    # do fields differ?
    diffkeys = tbls[0].diffkeys(tbls[1])
    if not diffkeys:
      return outsep.join(out)
    out.append('table mismatch: %s of %s compared fields differ' % (
      len(diffkeys),len(cmpkeys)))
    # compare field contents
    for key in diffkeys:
      # assign to tmp table
      keytbl = UniTable()
      for label,tbl in zip(labels,tbls):
        keytbl[label] = tbl[key]
      # field difference details
      detail = keytbl.diffkeys_explore(labels[0],labels[1],dump_sample=dump_sample)
      section = ['field %r:' % (key)] + detail
      out2 = ' '.join(section)
      if len(out2) > 99:
        out2 = '\n  '.join(section)
      out.append(out2)
    return outsep.join(out)



  # string/repr methods

  def __str__(self): return self.to_pptbl_str()
  def __repr__(self): return self.summary()

  def summary(self,sepline='\n  ',blocksep='-'*72):
    '''returns summary of table formats and storage size'''
    klass = self.__class__.__name__
    fmts = self.get_type_codes()
    recsize = sum(values.itemsize for values in self.values() if values is not None)
    out = ['%s summary:' % klass]

    out.append(blocksep)
    out.append('%s format summary: %s fields, %s bytes/record' \
      % (self.__class__.__name__,len(self.keys()),recsize))
    for i,(key,fmt) in enumerate(zip(self.keys(),fmts)):
      try:
        if fmt[0] == 'a':
          fmt = 'Char(%s)' % fmt[1:]
      except:
        pass
      out.append('  field %2s: %-9s %r' % (i+1,fmt,key))

    out.append(blocksep)
    out.append('%s data summary: %s records, %s bytes total' \
      % (klass,len(self),len(self)*recsize))
    for name in self._keys:
      value = self._data[name]
      out.append('  %-12s= %s' % (name,str(value)))
    
    out.append(blocksep)
    extra = self._summhook()
    if len(extra):
      out.extend(extra.split('\n'))
      out.append(blocksep)
    return sepline.join(out)

  def _summhook(self):
    return ''


  # logical operators from numpy
  # - these have no equivalent python infix operator
  # - defined here for convenience

  logical_and	= na.logical_and
  logical_or	= na.logical_or
  logical_xor	= na.logical_xor
  logical_not	= na.logical_not

  # setting of numarray error modes
  # - defined here for convenience

  # TODO: These don't seem to have a direct numpy equivalent. Commenting out
  # for now. Is na.error equivalent to numpy.seterr()?
  #Error		= na.error 
  #BufferError	= na.libnumarray.error

  # other useful array functions
  # - defined here for convenience

  ones		= staticmethod(na.ones)
  zeros		= staticmethod(na.zeros)
  arange	= staticmethod(na.arange)
  alltrue	= staticmethod(na.alltrue)
  sometrue	= staticmethod(na.sometrue)
  allclose	= staticmethod(na.allclose)
  searchsorted	= staticmethod(na.searchsorted)

####################################

def from_nab_file(filename):
  return UniTable().from_nab_file(filename)

fromfile = from_nab_file

#################################################################

if __name__ == "__main__":
  import doctest
  flags =  doctest.NORMALIZE_WHITESPACE
  flags |= doctest.ELLIPSIS
  flags |= doctest.REPORT_ONLY_FIRST_FAILURE
  doctest.testmod(optionflags=flags)

