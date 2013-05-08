"""Sample UniTable naive-bayes builder

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

from itertools import izip
from augustus.unitable import UniTable

import logging
logging.basicConfig()
log = logging.getLogger('naive-bayes')
log.setLevel(1)

#################################################################

class CountsBase(object):
  """base class for naive bayes count table functions
  """
  def __init__(self,threshold=1.0e-9):
    self.threshold = threshold
    self.reset()

  def reset(self):
    self._counts = {}
    self._probs = {}
    self._condcounts = {}
    self._condprobs = {}

  def set_target(self,tval,count,total):
    self._counts[tval] = count
    self._probs[tval] = float(count)/total

  def get_count(self,tval,default=0): return self._counts.get(tval,default)
  def get_prob(self,tval,default=0): return self._probs.get(tval,default)

  def set_conditional(self,tval,ikey,ival,ccount,ctotal):
    if not ccount:
      cprob = self.threshold
    else:
      cprob = float(ccount)/ctotal
    self._set_value(self._condcounts,tval,ikey,ival,ccount)
    self._set_value(self._condprobs,tval,ikey,ival,cprob)

  def get_condcount(self,tval,ikey,ival,default=0):
    return self._get_value(self._condcounts,tval,ikey,ival,default)
  def get_condprob(self,tval,ikey,ival,default=0.0):
    return self._get_value(self._condprobs,tval,ikey,ival,default)

  @staticmethod
  def _set_value(container,k1,k2,k3,value):
    a = container.setdefault(k1,{})
    b = a.setdefault(k2,{})
    b[k3] = value

  @staticmethod
  def _get_value(container,k1,k2,k3,default=1.0):
    a = container.get(k1,{})
    b = a.get(k2,{})
    return b.get(k3,default)

  def all_tval(self):
    return sorted(self._counts.keys())

  def all_ikey(self):
    out = set()
    for a in self._condcounts.values():
      out.update(a.keys())
    return sorted(out)

  def all_ival(self,ikey):
    out = set()
    for a in self._condcounts.values():
      b = a.get(ikey,{})
      out.update(b.keys())
    return sorted(out)

  def iter_ikv(self):
    for ikey in self.all_ikey():
      for ival in self.all_ival(ikey):
        yield (ikey,ival)

  def __str__(self):
    out = []
    out.append(str(self.tbl_counts()))
    out.append(str(self.tbl_probs()))
    return '\n'.join(out)

  def tbl_counts(self):
    return self._make_tbl(cfunc=self.get_count,ccfunc=self.get_condcount)

  def tbl_probs(self):
    return self._make_tbl(cfunc=self.get_prob,ccfunc=self.get_condprob)

  def _make_tbl(self,cfunc,ccfunc):
    out = UniTable()
    ikvlist = list(self.iter_ikv())
    out['__fld__'] = ['']+[ikv[0] for ikv in ikvlist]
    out['__val__'] = ['']+[ikv[1] for ikv in ikvlist]
    for tval in self.all_tval():
      value = cfunc(tval)
      ikv_vals = [ccfunc(tval,ikey,ival) for (ikey,ival) in ikvlist]
      out[str(tval)] = [value]+ikv_vals
    return str(out)
    


#################################################################

class NaiveBayesModel(CountsBase):
  def __init__(self,filename,target,inputs=None,threshold=1.0e-9):
    CountsBase.__init__(self,threshold=threshold)
    if isinstance(filename,UniTable):
      data = filename
    else:
      data = UniTable().fromfile(filename)
    self.model_build(data,target,inputs)

    self.verify_result = verify = UniTable()
    verify['orig'] = data[self.target] 
    verify['pred'] = self.model_predict(data)
    verify['agree'] = verify['orig'] == verify['pred']
    self.accuracy = float(verify['agree'].sum())/len(data)


  ###############################################################
  # model building

  def model_build(self,data,target,inputs=None):
    """build model using training dataset
    """
    self.reset()
    log.info('preparing to build model (training data: %s records)',len(data))
    # allow numeric index for target field
    if isinstance(target,int):
      target = data.keys()[target]
    # default inputs to all non-target fields
    if not inputs:
      inputs = data.keys()
      try:
        inputs.remove(target)
      except ValueError:
        raise ValueError, 'target field %r not in %r' % (target,inputs)
    self.target = target
    self.inputs = inputs

    log.info('preparing to build counts')
    data.sort_on(target)
    for tval,subtbl in data.subtbl_groupby(target):
      log.info('::building counts for target value: %s=%s (occurs in %s/%s records)',
                target,tval, len(subtbl),len(data))
      log.debug('::subtbl{%s}\n%s',tval,str(subtbl))
      self.set_target(tval,len(subtbl),len(data))
      for ikey in inputs:
        for ival in set(subtbl[ikey]):
          ccount = (subtbl[ikey] == ival).sum()
          self.set_conditional(tval,ikey,ival,ccount,len(subtbl))
          log.debug('::::subtbl{%s}: ikey=%s ival=%s ccount=%s ctotal=%s',tval,
            ikey,ival,ccount,len(subtbl))
    log.info('finished building model')
      

  ###############################################################
  # model scoring

  def model_predict(self,data):
    """return predicted value for each record in dataset
    """
    log.info('preparing to score model (input data: %s records)',len(data))
    tvals = self.all_tval()
    ikeys = [ikey for ikey in self.all_ikey() if data.has_key(ikey)]
    out = []
    row_predict = self._row_predict
    for row in data:
      ivals = [row[ikey] for ikey in ikeys]
      out.append(row_predict(tvals,ikeys,ivals))
      if len(out) % 1000 == 0:
        log.info('progress: %7.5f%% (%s/%s)',100.0*len(out)/len(data),len(out),len(data))
    log.info('finished scoring (output: %s results)',len(out))
    return out

  def _row_predict(self,tvals,ikeys,ivals):
    likelihoods = []
    # for each possible predicted value
    for tval in tvals:
      condprobs = [self.get_condprob(tval,ikey,ival) for (ikey,ival) in zip(ikeys,ivals)]
      lval = self.get_prob(tval)
      for cprob in condprobs:
        lval *= cprob
      likelihoods.append(lval)
    lsum = sum(likelihoods)
    scores = [float(lval)/lsum for lval in likelihoods]
    best = max(scores)
    i = scores.index(best)
    return tvals[i]


#################################################################

def main():
  """handle user command when run as top level program"""
  from optparse import OptionParser, make_option

  usage = 'usage: %prog [options] [datafile]'
  version = "%prog 0.0 alpha"
  option_list = [
    make_option('-v','--verbose',action='count',default=1,help="make progress output more verbose"),
    make_option('-q','--quiet',action='store_false',dest='verbose',help="no progress messages"),
    make_option('-t','--target',metavar='FLD',default=0,help="predict target field FLD"),
    make_option('-i','--inputs',metavar='FLDS',help="train using input fields FLDS"),
    make_option('-d','--dump',action='store_true',help="dump model data"),
    make_option('-c','--check',action='store_true',help="print accuracy check results"),
  ]

  parser = OptionParser(usage=usage,version=version,option_list=option_list)
  (opt,args) = parser.parse_args()

  log.setLevel(max(1,40 - (opt.verbose*10)))

  assert len(args) == 1, 'filename arg required'

  if opt.inputs:
    opt.inputs = opt.inputs.split(',')

  model = NaiveBayesModel(args[0],target=opt.target,inputs=opt.inputs)
  if opt.dump:
    print model
  if opt.check:
    print model.verify_result
  print 'accuracy = %7.5f%%' % (100.0*model.accuracy)

#################################################################

if __name__ == "__main__":
  main()

#################################################################
# vim:sw=2:sts=2:expandtab:shiftround
