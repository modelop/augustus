"""UniTable Input/Output commandline utility

Reads tables from CSV or binary files with autodetection of file type.
Writes tables as CSV, binary, or pretty-printed formats.

If an output filename is specified, then the output format
defaults to input format unless overridden (--bin,--csv,--tbl,--html,--xml).

If no output file is specified, default format is pretty-print (--tbl).

The input and output files may be the same to change the given file.

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
import re
from optparse import OptionParser, make_option
import itertools as it

from asarray import get_format, get_bestfit, count_masked
from unitable import UniTable
import augustus.const as AUGUSTUS_CONSTS


import logging
logging.basicConfig()
log = logging.getLogger('unitable')
log.setLevel(1)

# for command line args that are lists, allow either
# comma or whitespace to separate items
argsep = re.compile(r'[,\s]+')

########################################################################

class TableHandler(object):
  """Base class for object that operates on a loaded table"""

  option_list = []
  argsep = re.compile(r'[,\s]+')

  def handle(self,tbl,opt=None):
    pass

  @classmethod
  def argsplit(self,arg):
    if arg is None:
      return None
    return self.argsep.split(arg.strip())

########################################################################
########################################################################

class TblSaveSpace(TableHandler):
  """Try to save space in a table by adjusting the data type
  to smallest possible given current data.

  >>> handler = TblSaveSpace()
  >>> (opt,args) = OptionParser(option_list=handler.option_list).parse_args(['--save-space'])
  >>> data = UniTable(keys=['aaa','int8','ints','floats','bools'])
  >>> data['aaa'] = ['aaaaaaa','bb','cc']
  >>> data['int8'] = [99,123,-99]
  >>> data['ints'] = [999999,1234567,89]
  >>> data['floats'] = [9.99999,123.4567,8.9]
  >>> data['bools'] = [1,1,0]
  >>> print data.get_type_codes()
  ['a7', 'Int32', 'Int32', 'Float64', 'Int32']
  >>> out = handler.handle(data,opt)
  >>> print out.get_type_codes()
  ['a7', 'Int8', 'Int32', 'Float64', 'Bool']

  """

  option_list = [
    make_option('--save-space',action='store_true',
        help="reduce field types to smallest possible for current data"),
  ]

  @classmethod
  def handle(self,tbl,opt):
    if opt.save_space is None:
      return

    for key,arr in tbl.items():
      oformat = get_format(arr)
      nformat = get_bestfit(arr)
      if oformat != nformat:
        log.info('converting field %s from type %r to %r',key,oformat,nformat)
        tbl[key] = arr.astype(nformat)

    return tbl


########################################################################

class TblExpandCount(TableHandler):
  """Given a table that has an enumeration count column,
  expand the table by repeating each row for corresponding
  row count, and remove the count column.

  >>> handler = TblExpandCount()
  >>> (opt,args) = OptionParser(option_list=handler.option_list).parse_args(['-X'])
  >>> data = UniTable(keys=('_count_','abcde','qwerty'))
  >>> data['_count_'] = [1,3,2]
  >>> data['abcde'] = ['aa','bb','cc']
  >>> data['qwerty'] = [5,1,99.0]
  >>> print data
  +-------+-----+------+
  |_count_|abcde|qwerty|
  +-------+-----+------+
  |      1|   aa|   5.0|
  |      3|   bb|   1.0|
  |      2|   cc|  99.0|
  +-------+-----+------+
  >>> out = handler.handle(data,opt)
  >>> print out
  +-----+------+
  |abcde|qwerty|
  +-----+------+
  |   aa|   5.0|
  |   bb|   1.0|
  |   bb|   1.0|
  |   bb|   1.0|
  |   cc|  99.0|
  |   cc|  99.0|
  +-----+------+

  """

  option_list = [
    make_option('-x','--expand',metavar='FLD',
        help="expand tbl by repeating each row by count in FLD, remove FLD from tbl"),
    make_option('-X','--expand-count',action='store_const',const='_count_',dest='expand',
        help="shorthand for --expand=_count_"),
  ]

  @staticmethod
  def handle(tbl,opt):
    if opt.expand is None:
      return

    cntkey = opt.expand
    keys = tbl.keys()[:]
    try:
      keys.remove(cntkey)
    except ValueError:
      log.warning('cannot expand table, has no count column: %r',cntkey)
      return

    ntbl = UniTable(keys=keys)
    xcnt = tbl[cntkey]
    for key in keys:
      oval = tbl[key]
      nval = []
      for cnt,val in it.izip(xcnt,oval):
        for i in range(cnt):
          nval.append(val)
      ntbl[key] = nval
    return ntbl

########################################################################

class TblFormatSummary(TableHandler):
  """Print summary of the storage formats.

  >>> handler = TblFormatSummary()
  >>> (opt,args) = OptionParser(option_list=handler.option_list).parse_args(['--summary'])
  >>> data = UniTable()
  >>> data['a'] = [1,3,2]
  >>> data['b'] = ['aaaaa','bb','cc']
  >>> data['c'] = [5,1,99.0]
  >>> data['d'] = [0,1,0]
  >>> data['e'] = [2345998,1,0]
  >>> data['f'] = [2,10000,0]
  >>> print data
  +-+-----+----+-+-------+-----+
  |a|  b  | c  |d|   e   |  f  |
  +-+-----+----+-+-------+-----+
  |1|aaaaa| 5.0|0|2345998|    2|
  |3|   bb| 1.0|1|      1|10000|
  |2|   cc|99.0|0|      0|    0|
  +-+-----+----+-+-------+-----+
  >>> assert handler.handle(data,opt) == False
  3 records, 6 fields, 29 bytes/record, 87 bytes total
  +-+-----+--------+----+
  |@|field|typecode|size|
  +-+-----+--------+----+
  |1|    a|   Int32|   4|
  |2|    b| Char(5)|   5|
  |3|    c| Float64|   8|
  |4|    d|   Int32|   4|
  |5|    e|   Int32|   4|
  |6|    f|   Int32|   4|
  +-+-----+--------+----+

  """

  option_list = [
    make_option('-a','--summary',action='store_true',help="print table content summary"),
  ]

  @classmethod
  def handle(self,tbl,opt):
    if opt.summary is None:
      return
    recsize = sum(col.itemsize() for col in tbl.values())
    recs = len(tbl)
    sum_str = '%s records, %s fields, %s bytes/record, %s bytes total' \
                % (recs,len(tbl.keys()),recsize,recs*recsize)
    print sum_str
    print self.fld_summary(tbl)
    return False

  @staticmethod
  def fld_summary(tbl):
    out = UniTable()
    keys = tbl.keys()
    out['@'] = range(1,len(keys)+1)
    out['field'] = keys
    out['typecode'] = [re.sub(r'^a(.*)',r'Char(\1)',fmt) for fmt in tbl.get_type_codes()]
    out['size'] = [col.itemsize() for col in tbl.values()]
    nmasked = [count_masked(col) for col in tbl.values()]
    if sum(nmasked):
      out['masked'] = [str(n) for n in nmasked]
      masked = out['masked']
      masked[masked == '0'] = ''
    return out

########################################################################

class TblWhere(TableHandler):
  """Select specified rows from table.

  """

  option_list = [
    make_option('--where',metavar='EXPR',
        help="select rows from table where EXPR is True"),
  ]

  @classmethod
  def handle(self,tbl,opt):
    if opt.where is None:
      return

    expr = opt.where
    mask = tbl.eval(expr)
    return tbl.subtbl(mask)

########################################################################

class TblSelect(TableHandler):
  """Select specified fields from table.

  """

  option_list = [
    make_option('--select',metavar='FIELDS',
        help="select FIELDS from table"),
  ]

  @classmethod
  def handle(self,tbl,opt):
    if opt.select is None:
      return

    fields = self.argsplit(opt.select.strip())
    idx = getattr(opt,'add_index',None)
    if idx:
      fields = [idx] + fields
    return tbl.copy(*fields)

########################################################################

class TblSlice(TableHandler):
  """Sample table data given start:stop:stride

  """

  option_list = [
    make_option('--slice',metavar='START:STOP:STRIDE',
      help="select subset of records from table"),
  ]

  @classmethod
  def handle(self,tbl,opt):
    if opt.slice is None or not len(tbl):
      return

    args = []
    for arg in opt.slice.split(':'):
      if not arg:
        args.append(None)
      else:
        args.append(int(arg))
    pyslice = slice(*args)
    takeidx = range(*pyslice.indices(len(tbl)))
    out = tbl.subtbl(takeidx)
    if len(tbl) == len(out):
      return
    log.info('taking %s reduced output from %s to %s records',
      str(pyslice),len(tbl),len(out))
    return out

########################################################################

class TblAddIndex(TableHandler):
  """Add a numbered index column to table

  """

  option_list = [
    make_option('--add-index',metavar='NAME',
      help="add index column as NAME to table"),
    make_option('-I',action='store_const',const='_idx_',dest='add_index',
      help="shorthand for --add-index=_idx_"),
  ]

  @classmethod
  def handle(self,tbl,opt):
    if opt.add_index is None:
      return

    out = UniTable()
    out[opt.add_index] = range(len(tbl))
    out.update(tbl)
    return out

########################################################################

class TblSort(TableHandler):
  """Sort table by specified field

  """

  option_list = [
    make_option('--sort-on',metavar='NAME',help="sort table on NAME field"),
    make_option('--reverse','-R',action='store_true',default=False,
      help="modify --sort-on option to reverse results"),
  ]

  @classmethod
  def handle(self,tbl,opt):
    if opt.sort_on is None:
      return

    return tbl.sorted_on(opt.sort_on,reverse=opt.reverse)

########################################################################

class TblExec(TableHandler):
  """Execute arbitrary expression suite in table context.

  Unlike the rules in an evaltable, the expression can contain
  multiple assignments and even change the contents of an
  existing field.  For example,

  "err=act-pred; abserr=_.abs(err); foo+=2; bar/=foo"

  Since the typical use of this option is to add fields to the table,
  it is executed before other options such as --where, etc.

  """

  option_list = [
    make_option('-E','--eval',metavar='EXPR',
      help="execute python EXPR in table context"),
  ]

  @classmethod
  def handle(self,tbl,opt):
    if opt.eval is None:
      return

    return tbl.eval(opt.eval)

########################################################################

class TblQuery(TableHandler):
  """Print arbitrary expression evaluated in table context.

  The expression may be arbitrarily complex, but must return a
  value that can be printed and may not contain assignments.

  This option terminates processing of the table, so it is executed
  after all other options.

  """

  option_list = [
    make_option('-Q','--query',metavar='EXPR',
      help="print results of python EXPR in table context"),
  ]

  @classmethod
  def handle(self,tbl,opt):
    if opt.query is None:
      return

    out = tbl.eval(opt.query)
    if out is not None:
      print out
    return False

########################################################################

class TblDiffKeys(TableHandler):
  """Print keys where data differs between two tables

  """

  option_list = [
    make_option('--diff',metavar='REF',help="print differences from REF file or directory"),
    make_option('--diff-dump',metavar='N',default=10,type='int',
      help="modify diff output to also print first N differing records (default=%default)"),
  ]

  @classmethod
  def handle(self,tbl,opt):
    if opt.diff is None:
      return
    if os.path.isdir(opt.diff):
      trylocs = [os.path.join(opt.diff,os.path.basename(opt.input))]
      if not os.path.isabs(opt.input) and os.path.dirname(opt.input):
        trylocs.append(os.path.join(opt.diff,opt.input))
      for other in trylocs:
        if os.path.exists(other):
          break
    else:
      other = opt.diff
    # prepare labels, giving preference to basenames if unique
    filename = [other,opt.input]
    label = [os.path.basename(x) for x in filename]
    if label[0] == label[1]:
      label = filename
    ref = UniTable().fromfile(other)
    out = ref.diff(tbl,label1=label[0],label2=label[1],dump_sample=opt.diff_dump)
    if out:
      print '***** files differ: %s %s' % tuple(filename)
      print out
      print
      # allow multi-file processing
      # sys.exit(1)
    return False

########################################################################
########################################################################

class TableMaster(object):
  """Table Input/Output utility

  Reads tables from CSV or binary files with autodetection of file type.
  Writes tables as CSV, binary, or pretty-printed formats.
  Keeps track of input format so that it may be applied to output.

  This class may be used as-is for conversion of table format,
  or may be subclassed for application that transform table contents.
  The tbl_klass (typically UniTable) must inherit from TableMethods.

  """
  option_list = [
    make_option('-i','--input',metavar='FILE',
        help="input from FILE (required for binary input," \
            " default <stdin> for others, format is detected automatically)"),
    make_option('-o','--output',metavar='FILE',
        help="output to FILE (required for binary output," \
            " default <stdout> for others," \
            " default format is to preserve the input format)"),

    #make_option('-f','--format',metavar='FMT',
    #    help="output FMT [csv,bin,tbl] (default: retain input FMT)"),
    make_option('--csv',action='store_const',const='csv',dest='format',
        help="output CSV format"),
    make_option('--bin',action='store_const',const='bin',dest='format',
        help="output binary format"),
    make_option('--tbl',action='store_const',const='tbl',dest='format',
        help="output pretty-printed table"),
    make_option('--tty',action='store_const',const='tty',dest='format',
        help="output pretty-printed table with header repeated as needed"
            " to always be visible in current terminal window"),
    make_option('--html',action='store_const',const='html',dest='format',
        help="output HTML table"),
    make_option('--xml',action='store_const',const='xml',dest='format',
        help="output XML table"),

    make_option('-s','--sep',metavar='SEP',help="use field separator SEP for CSV output"),
    make_option('-S','--insep',metavar='SEPS',
        help="consider any value in SEPS string as possible field delimiter for CSV input"),

    make_option('--broken-csv',action='store_true',
        help="use field splitting instead of csv.py" \
            " (needed to read some malformed files that contain newlines in the records," \
            " note that this disables any attempt to handle quotes,escapes,etc)"),
    make_option('--noconvert',action='store_true',
        help="keep CSV imported data as strings, disabling all data conversion"),

    make_option('--trymasked',action='store_true',
        help="allow use of masked arrays to handle missing values in CSV imported data"),
    make_option('--masked-values',metavar='VALS',default='',
        help="list of values to mask when converting CSV imported data" \
            " (default: empty string)"),

    make_option('--index-fields',metavar='FLDS',default='',
        help="list of text fields to be replaced by an index into a symbol table"),
    make_option('--index-file',metavar='FILENAME',
        help="name of CSV file keeping symbol table" \
            " (if it exists, it will be preserved and extended)"),

  ]
  tbl_klass = UniTable

  def __init__(self,handlers=[],opt=None,args=[]):
    self.handlers = handlers
    if opt is None:
      # just take defaults
      parser = OptionParser(option_list=self.option_list)
      (opt,args) = parser.parse_args([])
    if opt.input is None and len(args) > 0:
      opt.input = args[0]
      args = args[1:]
    if len(args) > 0 and opt.output:
      log.warning('ignoring extra args: %r',args)
      args = []
    if opt.input is None:
      print >>sys.stderr, 'reading CSV from <stdin>.....'
    opt.args = args
    self.opt = opt

    self.iformat = None
    self.csv_dialect = None

  def run(self):
    opt = self.opt
    if opt.args:
      print opt.input
    self.run1()
    while opt.args:
      opt.input = opt.args[0]
      opt.args = opt.args[1:]
      print opt.input
      sys.stdout.flush()
      self.run1()
    
  def run1(self):
    tbl = self.load()
    for handler in self.handlers:
      out = handler.handle(tbl,self.opt)
      if out is False:
        return
      if out is not None:
        tbl = out
    self.write(tbl)

  def load(self):
    load_opts = dict(
      broken_csv=self.opt.broken_csv,
      noconvert=self.opt.noconvert,
      trymasked=self.opt.trymasked,
      index_file=self.opt.index_file,
      insep=self.opt.insep,
    )
    if self.opt.masked_values:
      load_opts['masked_values'] = argsep.split(self.opt.masked_values.strip())
    if self.opt.index_fields:
      load_opts['index_fields'] = argsep.split(self.opt.index_fields.strip())

    tbl = self.tbl_klass()
    tbl.from_any_file(self.opt.input,**load_opts)
    self.iformat = tbl.get_original_format()
    self.csv_dialect = tbl.get_csv_dialect()
    return tbl

  def write(self,tbl):
    opt = self.opt
    if not len(tbl) and opt.output is None:
      # do not print empty table frame to stdout
      return
    format = opt.format
    if format is None:
      if opt.output is not None:
        format = self.iformat
      else:
        format = 'tbl'
    if format is not None:
      format = str(format).lower()
    if format == 'bin':
      tbl.to_nab_file(self.opt.output)
    elif format == 'csv':
      tbl.to_csv_file(self.opt.output,sep=self.opt.sep)
    elif format == 'tbl':
      tbl.to_pptbl_file(self.opt.output)
    elif format == 'html':
      tbl.to_html_file(self.opt.output)
    elif format == 'xml':
      tbl.to_xml_file(self.opt.output)
    elif format == 'tty':
      tbl.to_pptbl_file(self.opt.output,rhead=self.tty_lines())
    else:
      log.error('unknown output format: %r',format)
      assert False

  def tty_lines(self,default=32):
    try:
      import fcntl, termios, struct
      buf = fcntl.ioctl(0, termios.TIOCGWINSZ, ' '*4)
      y,x = struct.unpack('hh',buf)
      return max(2,y-4)
    except:
      return default

#################################################################
# list of options/functions bundles to add to user interface

handlers = [
  TblExpandCount,
  TblAddIndex,		# after ExpandCount; before Where,Slice,Sort
  TblExec,		# before Where (may add new fields)
  TblWhere,		# before Select (may need non-selected fields)
  TblSelect,
  TblSlice,
  TblSort,
  TblQuery,		# terminal
  TblDiffKeys,		# terminal
  TblSaveSpace,
  TblFormatSummary,	# terminal
]

#################################################################

def selftest(verbose=0):
  log.warning('selftests may report false errors on non-linux or non-32 bit machines')
  testmods = ['asarray', 'storage', 'unitable', 'evaltbl', 'rules', 'wrappers']
  for mod in testmods:
    __import__(mod)

  import unittest
  import doctest
  if not verbose:
    doctest.set_unittest_reportflags(doctest.REPORT_ONLY_FIRST_FAILURE)
  flags =  doctest.NORMALIZE_WHITESPACE | doctest.ELLIPSIS

  suite = unittest.TestSuite()
  for mod in testmods:
    suite.addTest(doctest.DocTestSuite(mod,optionflags=flags))
  runner = unittest.TextTestRunner()
  return runner.run(suite)

#################################################################
#################################################################

def main():
  """handle user command when run as top level program"""
  from optparse import OptionParser, make_option

  usage = 'usage: %prog [options] [datafile]'
  version = "%prog " + AUGUSTUS_CONSTS._AUGUSTUS_VER
  option_list = [
    make_option('-v','--verbose',action='count',default=1,help="make progress output more verbose"),
    make_option('-q','--quiet',action='store_false',dest='verbose',help="no progress messages"),
    make_option('--selftest',action='store_true',help="run module doctest"),
  ]

  option_list.extend(TableMaster.option_list)
  for handler in handlers:
    option_list.extend(handler.option_list)

  parser = OptionParser(usage=usage,version=version,option_list=option_list)
  (opt,args) = parser.parse_args()

  log.setLevel(max(1,40 - (opt.verbose*10)))

  if opt.selftest:
    return selftest(verbose=opt.verbose>1)

  master = TableMaster(handlers=handlers,opt=opt,args=args)
  master.run()

#################################################################

if __name__ == "__main__":
  main()

#################################################################
