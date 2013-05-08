"""BaselineModel discrete-mode sample implementation


"""

__copyright__ = """
Copyright (C) 2005-2006  Open Data ("Open Data" refers to
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


import sys, re
from augustus.unitable import UniTable, EvalTable, Rules, Rule
from augustus.unitable.veclib import veclib

import logging
logging.basicConfig()
log = logging.getLogger('cntmodel')
log.setLevel(1)

########################################################################

class CountTable(object):
  """Table of event counts

    >>> tbl = CountTable(
    ...       ('Male','Female'),
    ...       ('Sandals','Sneakers','Leather','Boots','Other'),
    ...       ((6,17,13,9,5),(13,5,7,16,9)))
    >>> print tbl
    +--------+-------+--------+-------+-----+-----+--------+
    |  (#)   |Sandals|Sneakers|Leather|Boots|Other|_totals_|
    +--------+-------+--------+-------+-----+-----+--------+
    |    Male|      6|      17|     13|    9|    5|      50|
    |  Female|     13|       5|      7|   16|    9|      50|
    |_totals_|     19|      22|     20|   25|   14|     100|
    +--------+-------+--------+-------+-----+-----+--------+

    >>> print tbl.null_hypothesis()
    +--------+-------+--------+-------+-----+-----+--------+
    |  (#)   |Sandals|Sneakers|Leather|Boots|Other|_totals_|
    +--------+-------+--------+-------+-----+-----+--------+
    |    Male|    9.5|    11.0|   10.0| 12.5|  7.0|    50.0|
    |  Female|    9.5|    11.0|   10.0| 12.5|  7.0|    50.0|
    |_totals_|   19.0|    22.0|   20.0| 25.0| 14.0|   100.0|
    +--------+-------+--------+-------+-----+-----+--------+

    >>> print tbl.chi_squared()
    14.0272590567



  """
  def __init__(self,rows,cols,data=None):
    self.rows = rows or ['1']
    self.cols = cols or ['1']
    self._len = len(self.rows)*len(self.cols)
    if data is None:
      data = [0]*len(self)
    elif isinstance(data,CountTable):
      data = data.matrix
    data = veclib.asarray(data)
    data.shape = (len(self.rows),len(self.cols))
    self.matrix = data

  def chi_squared(self,baseline=None):
    '''return chi_squared relative to given baseline table'''
    tbl = self.chi_squared_rawtbl(baseline=baseline)
    return tbl.matrix.sum()

  def chi_squared_rawtbl(self,baseline=None):
    '''return chi_squared relative to given baseline table'''
    if baseline is None:
      baseline = self.null_hypothesis()
    if isinstance(baseline,CountTable):
      baseline = baseline.matrix
    observed = self.matrix.astype("Float")
    return self.new(((observed-baseline)**2)/baseline)

  def null_hypothesis(self):
    '''return absolute null hypothesis for this table'''
    out = veclib.repeat(self.row_sums(),len(self.cols)).astype("Float")
    out.shape = self.matrix.shape
    out *= self.col_sums()/float(self.sum())
    return self.new(out)

  def row_sums(self): return veclib.sum(self.matrix,1)
  def col_sums(self): return veclib.sum(self.matrix,0)
  def sum(self): return self.matrix.sum()

  def copy(self): return self.new(self.matrix.copy())
  def new(self,data=None): return CountTable(self.rows,self.cols,data)
  def __len__(self): return self._len

  def __iadd__(self,other):
    if not isinstance(other,CountTable): other = self.new(other)
    self.matrix += other.matrix
    return self

  def __isub(self,other):
    if not isinstance(other,CountTable): other = self.new(other)
    self.matrix -= other.matrix
    return self

  def __add__(self,other):
    if not isinstance(other,CountTable): other = self.new(other)
    return self.new(self.matrix+other.matrix)

  def __sub__(self,other):
    if isinstance(other,CountTable):
      other = other.matrix
    return self.new(self.matrix-other)

  def __str__(self):
    out = UniTable()
    out['(#)'] = list(self.rows) + ['_totals_']
    col_sums = self.col_sums()
    for i,col in enumerate(self.cols):
      out[col] = list(self.matrix[:,i]) + [col_sums[i]]
    out['_totals_'] = list(self.row_sums()) + [self.sum()]
    return str(out)

########################################################################

class FieldExprList(object):
  """List of field expressions, typically passed in from commandline"""

  _assign_pat = re.compile(r'([a-zA-Z_]\w*)\s*=\s*(.*)$')
  _name_pat = re.compile(r'[a-zA-Z_]\w*$')

  def __init__(self,*exprlist):
    self._keys = keys = []
    self._rhs = rhs = []
    for expr in exprlist:
      m = self._assign_pat.match(expr)
      if m:
        key,rhs = m.groups()
      else:
        key = rhs = expr
      keys.append(key)
      rhs.append(rhs)

  def keys(self): return self._keys[:]
  def values(self): return self._rhs[:]
  def items(self): return zip(self._keys,self._rhs)

  def rules(self,target,baresub='%s.astype("Bool")'):
    rhslist = self.values()
    if baresub:
      for i,rhs in enumerate(rhslist):
        if self._name_pat.match(rhs):
          rhslist[i] = baresub % rhs
    out = '%s = %s' % (target,rhs)
    return out


class FieldExpr(object):
  _name_pat = re.compile(r'[a-zA-Z_]\w*$')
  def __init__(self,expr):
    self.expr = expr

  def make_rule(self,target):
    rhs = self.expr
    if self._name_pat.match(rhs):
      rhs = '%s.astype("Bool")' % rhs
    out = '%s = %s' % (target,rhs)
    return out

########################################################################

class UniCounter(object):
  """UniTable CountTable extractor

  """
  def __init__(self,rows,cols):
    self.rows = rows
    self.cols = cols
    self.rules = self.make_rules()

  def row_labels(self,prefix='_r'): return self._labels(len(self.rows),prefix)
  def col_labels(self,prefix='_c'): return self._labels(len(self.cols),prefix)
  def _labels(self,count,prefix): return [('%s%s' % (prefix,n)) for n in range(1,count+1)]
  def rc_labels(self):
    out = []
    for r in self.row_labels():
      for c in self.col_labels():
        rc = '%s%s' % (r,c)
        out.append(rc)
    return out

  def make_rules(self):
    row_labels = self.row_labels()
    col_labels = self.col_labels()
    # if _count_ is not in the table, provide a default
    rules = ['_count_ = 1']
    for r in row_labels:
      for c in col_labels:
        rc = '%s%s' % (r,c)
        rule = '%s = (%s & %s) * _count_' % (rc,r,c)
        rules.append(rule)
    rules.extend(self.make_expr_rules(self.rows,row_labels))
    rules.extend(self.make_expr_rules(self.cols,col_labels))
    return rules

  def make_expr_rules(self,expr_list,keys):
    rules = []
    for key,expr in zip(keys,expr_list):
      rule = FieldExpr(expr).make_rule(key)
      rules.append(rule)
    return rules

  def make_cnttbl(self,data=None):
    return CountTable(self.rows,self.cols,data)
      
  def load(self,*filenames):
    cnttbl = self.make_cnttbl()
    for filename in filenames:
      self.loadfile(filename,cnttbl)
    return cnttbl

  def loadfile(self,filename,cnttbl):
    rules = Rules(*self.rules)
    tbl = EvalTable(rules).fromfile(filename)
    counts = [tbl.field(rc).sum() for rc in self.rc_labels()]
    cnttbl += counts



########################################################################
########################################################################

def main():
  """handle user command when run as top level program"""
  from optparse import OptionParser, make_option

  usage = 'usage: %prog [options] [datafile]'
  version = "%prog 0.0 alpha"
  option_list = [
    make_option('-v','--verbose',action='count',default=1,help="make progress output more verbose"),
    make_option('-q','--quiet',action='store_false',dest='verbose',help="no progress messages"),
    make_option('--selftest',action='store_true',help="run module doctest"),

    make_option('-r','--rows',metavar='ROWS',default='1',help="select ROWS for CountTable"),
    make_option('-c','--cols',metavar='COLS',default='1',help="select COLS for CountTable"),
    make_option('-b','--baseline',metavar='FILE',default=[],help="select FILE to establish baseline"),

  ]

  parser = OptionParser(usage=usage,version=version,option_list=option_list)
  (opt,args) = parser.parse_args()

  log.setLevel(max(1,40 - (opt.verbose*10)))

  if opt.selftest or not args:
    import doctest
    doctest.testmod()
    return

  argsep = re.compile(r'[,\s]')
  if opt.rows:
    opt.rows = argsep.split(opt.rows)
  if opt.cols:
    opt.cols = argsep.split(opt.cols)
  if opt.baseline:
    opt.baseline = argsep.split(opt.baseline)

  loader = UniCounter(rows=opt.rows,cols=opt.cols)

  if opt.baseline:
    print 'BASELINE:', '(%s)' % ','.join(opt.baseline)
    baseline = loader.load(*opt.baseline)
    print baseline
    print
  else:
    baseline = None

  print
  print 'OBSERVED:', '(%s)' % ','.join(args)
  observed = loader.load(*args)
  print observed
  print

  if baseline is None:
    print 'BASELINE:', '(no baseline specified, using null hypothesis)'
    baseline = observed.null_hypothesis()
    print baseline
    print

  print 'DEBUG: Chi_Squared Calculation before summing:'
  print observed.chi_squared_rawtbl(baseline)
  print

  print 'Chi_Squared: %s' % observed.chi_squared(baseline)

if __name__ == "__main__":
  main()

#################################################################
# vim:sw=2:sts=2:expandtab:shiftround
