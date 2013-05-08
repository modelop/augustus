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
log = logging.getLogger('querytbl')
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
  def __init__(self,rownames,colnames,data=None):
    self.rownames = rownames or ['1']
    self.colnames = colnames or ['1']
    self._len = len(self.rownames)*len(self.colnames)
    if data is None:
      data = [0]*len(self)
    elif isinstance(data,CountTable):
      data = data.matrix
    data = veclib.asarray(data)
    data.shape = (len(self.rownames),len(self.colnames))
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
    out = veclib.repeat(self.row_sums(),len(self.colnames)).astype("Float")
    out.shape = self.matrix.shape
    out *= self.col_sums()/float(self.sum())
    return self.new(out)

  def row_sums(self): return veclib.sum(self.matrix,1)
  def col_sums(self): return veclib.sum(self.matrix,0)
  def sum(self): return self.matrix.sum()

  def copy(self): return self.new(self.matrix.copy())
  def new(self,data=None): return CountTable(self.rownames,self.colnames,data)
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

  def export(self):
    out = UniTable()
    out['(#)'] = list(self.rownames) + ['_totals_']
    col_sums = self.col_sums()
    for i,col in enumerate(self.colnames):
      out[col] = list(self.matrix[:,i]) + [col_sums[i]]
    out['_totals_'] = list(self.row_sums()) + [self.sum()]
    return out

  def __str__(self):
    return str(self.export())

########################################################################

class FieldExprList(object):
  """List of field expressions, typically passed in from commandline
  
  >>> x = FieldExprList('abcde','x=qwerty','y=_.bitwise_and(field,0x1234)')
  >>> print x
  +---+-----+---------------------------+---------------------------------+
  |key| name|            expr           |               rule              |
  +---+-----+---------------------------+---------------------------------+
  |_f1|abcde|                      abcde|       _f1 = abcde.astype("Bool")|
  |_f2|    x|                     qwerty|      _f2 = qwerty.astype("Bool")|
  |_f3|    y|_.bitwise_and(field,0x1234)|_f3 = _.bitwise_and(field,0x1234)|
  +---+-----+---------------------------+---------------------------------+


  """
  _assign_pat = re.compile(r'([a-zA-Z_]\w*)\s*=\s*([^=].*)$')
  _name_pat = re.compile(r'[a-zA-Z_]\w*$')

  def __init__(self,*args,**kwargs):
    keyprefix = kwargs.get('keyprefix','_f')
    self._keys = [('%s%s' % (keyprefix,n)) for n in range(1,len(args)+1)]
    self._names = names = []
    self._exprlist = exprlist = []
    for arg in args:
      m = self._assign_pat.match(arg)
      if m:
        name,rhs = m.groups()
      else:
        name = rhs = arg
      names.append(name)
      exprlist.append(rhs)

  def keys(self): return self._keys[:]
  def names(self): return self._names[:]
  def values(self): return self._exprlist[:]
  def items(self): return zip(self._keys,self._exprlist)

  def rules(self,namepat='%(key)s',baresub='%s.astype("Bool")'):
    rhslist = self.values()
    if baresub:
      for i,rhs in enumerate(rhslist):
        if self._name_pat.match(rhs):
          rhslist[i] = baresub % rhs
    out = []
    for i,(key,name,rhs) in enumerate(zip(self._keys,self._names,rhslist)):
      patdict = dict(i=i,key=key,name=name)
      lhs = namepat % patdict
      rule = '%s = %s' % (lhs,rhs)
      out.append(rule)
    return out

  def __str__(self):
    out = UniTable()
    out['key'] = self.keys()
    out['name'] = self.names()
    out['expr'] = self.values()
    out['rule'] = self.rules()
    return str(out)


########################################################################

class UniCounter(object):
  """UniTable CountTable extractor

  """
  def __init__(self,rows,cols):
    self.rows = FieldExprList(keyprefix='_r',*rows)
    self.cols = FieldExprList(keyprefix='_c',*cols)
    self.rules = self.make_rules()

  def make_rules(self):
    # if _count_ is not in the table, provide a default
    rules = ['_count_ = 1']
    for r in self.rows.keys():
      for c in self.cols.keys():
        rc = '%s%s' % (r,c)
        rule = '%s = (%s & %s) * _count_' % (rc,r,c)
        rules.append(rule)
    rules.extend(self.rows.rules())
    rules.extend(self.cols.rules())
    return rules

  def rc_labels(self):
    out = []
    for r in self.rows.keys():
      for c in self.cols.keys():
        rc = '%s%s' % (r,c)
        out.append(rc)
    return out

  def make_cnttbl(self,data=None):
    return CountTable(self.rows.names(),self.cols.names(),data)
      
  def load(self,*args):
    cnttbl = self.make_cnttbl()
    for arg in args:
      self._load(arg,cnttbl)
    return cnttbl

  def _load(self,arg,cnttbl):
    rules = Rules(*self.rules)
    tbl = EvalTable(rules).load(arg)
    counts = [tbl.field(rc).sum() for rc in self.rc_labels()]
    cnttbl += counts

########################################################################

class Main(object):
  def __init__(self,opt,*args):
    self.opt = opt
    self.args = args
    self.out = None

  def __call__(self):
    self.handle_opts(self.opt)
    for arg in self.args:
      self.handle_arg(self.opt,arg)
    return self.out

  @classmethod
  def handle_opts(self,opt):
    argsep = re.compile(r'[,\s]+')
    for name in ('rows','cols','baseline','select','where'):
      raw = getattr(opt,name,None)
      if raw:
        tmp = argsep.split(raw.strip())
        setattr(opt,name,tmp)
    
  @classmethod
  def handle_arg(self,opt,arg):
    tbl = UniTable().fromfile(arg)
    if opt.select:
      tbl = self.handle_select(opt,tbl)
    tbl = self.handle_counttable(opt,arg,tbl)
    print tbl.export().to_csv_str()
      
  @classmethod
  def handle_select(self,opt,tbl):
    fldexpr = FieldExprList(*opt.select)
    rules = fldexpr.rules()
    tbl = EvalTable(rules).update(tbl)
    out = UniTable()
    for key,name in zip(fldexpr.keys(),fldexpr.names()):
      out[name] = tbl[key]
    return out

  @classmethod
  def handle_counttable(self,opt,arg,tbl):
    loader = UniCounter(rows=opt.rows,cols=opt.cols)
    log.debug(' loader.rows:\n%s\n',loader.rows)
    log.debug(' loader.cols:\n%s\n',loader.cols)

    if opt.baseline:
      baseline = loader.load(*opt.baseline)
      label = ','.join(opt.baseline)
      log.info(' BASELINE: (%s)\n%s\n',label,baseline)
    else:
      baseline = None

    observed = loader.load(tbl)
    log.info(' OBSERVED: (%s)\n%s\n',arg,observed)

    if opt.test.lower() == 'chi_squared':
      if baseline is None:
        baseline = observed.null_hypothesis()
        label = 'no baseline specified, using null hypothesis'
        log.info(' BASELINE: (%s)\n%s\n',label,baseline)

      log.debug(' Chi_Squared Calculation before summing:\n%s\n',\
                observed.chi_squared_rawtbl(baseline))

      print 'Chi_Squared: %s' % observed.chi_squared(baseline)
    return observed


########################################################################
########################################################################

def main():
  """handle user command when run as top level program"""
  from optparse import OptionParser, make_option

  usage = 'usage: %prog [options] [datafile]'
  version = "%prog 0.0 alpha"
  option_list = [
    make_option('-v','--verbose',action='count',default=2,help="make progress output more verbose"),
    make_option('-q','--quiet',action='store_false',dest='verbose',help="no progress messages"),
    make_option('--selftest',action='store_true',help="run module doctest"),

    make_option('-s','--select',metavar='FIELDS',help="select FIELDS from incoming table"),

    make_option('-r','--rows',metavar='ROWS',default='1',help="select ROWS for CountTable"),
    make_option('-c','--cols',metavar='COLS',default='1',help="select COLS for CountTable"),
    make_option('-b','--baseline',metavar='FILE',default=[],help="select FILE to establish baseline"),
    make_option('-t','--test',metavar='TEST',default='',help="run TEST on selected data"),

  ]

  parser = OptionParser(usage=usage,version=version,option_list=option_list)
  (opt,args) = parser.parse_args()

  log.setLevel(max(1,40 - (opt.verbose*10)))

  if opt.selftest:
    import doctest
    doctest.testmod()
    return

  runner = Main(opt,*args)
  return runner()

if __name__ == "__main__":
  main()

#################################################################
# vim:sw=2:sts=2:expandtab:shiftround
