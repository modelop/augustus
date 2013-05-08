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

"""EvalTable: A UniTable supporting rule based field calculation.
  
  Rules are specified as simple python assignment expressions.
  When evaluated, the local namespace of a rule is the UniTable
  itself.  Variable referenced or assigned refer to unitable fields
  unless otherwise specified as parameters to the rule.

  Fields defined by rules are not calculated until/unless referenced.
  Dependencies between rules are handled automatically on demand.

  Note that file I/O does not preserve the rules - just the basic
  UniTable data is saved, including any dynamically calculated fields.
  To force calculation of all rule based fields, use preload_rules().

  Rules may be predefined before loading the table data, or
  added dynamically as needed.

  >>> rules = Rules('c=a+b','aa=a.astype("Float")','d=aa/c')
  >>> x = EvalTable(rules)
  >>> x['a'] = [2,5,9,5]
  >>> x['b'] = [0,-1,3,5]
  >>> print x
  +-+--+
  |a|b |
  +-+--+
  |2| 0|
  |5|-1|
  |9| 3|
  |5| 5|
  +-+--+
  >>> print x['c']
  [ 2  4 12 10]
  >>> print x
  +-+--+--+
  |a|b |c |
  +-+--+--+
  |2| 0| 2|
  |5|-1| 4|
  |9| 3|12|
  |5| 5|10|
  +-+--+--+
  >>> print x['d']
  [ 1.    1.25  0.75  0.5 ]
  >>> print x
  +-+--+--+---+----+
  |a|b |c | aa| d  |
  +-+--+--+---+----+
  |2| 0| 2|2.0| 1.0|
  |5|-1| 4|5.0|1.25|
  |9| 3|12|9.0|0.75|
  |5| 5|10|5.0| 0.5|
  +-+--+--+---+----+
  >>> x.reset_rules()
  >>> print x
  +-+--+
  |a|b |
  +-+--+
  |2| 0|
  |5|-1|
  |9| 3|
  |5| 5|
  +-+--+
  >>> print x['c']
  [ 2  4 12 10]

"""


import time
from unitable import UniTable
from rules import Rule, Rules, Expr

#################################################################

class EvalTable(UniTable):
  """Evaluation Table: a UniTable where some of the columns are
  computed dynamically.
  """
  def __init__(self,rules=None,*args,**kwargs):
    UniTable.__init__(self,*args,**kwargs)
    if not isinstance(rules,Rules):
      rules = Rules(rules)
    self._rules = rules
    self._rulearg = self.as_mapping()
    self._made = {}
    self._inprog = []

  def _new_hook(self):
    return EvalTable(rules=self._rules)

  def _grow_hook(self):
    # this is a blunt way to provide the functionality for now
    self.reset_rules()
    return self

  @property
  def rules(self): return self._rules

  def add_rule(self,rule,**context):
    '''add rule(s) to current rule set'''
    context.setdefault('__',self)
    return self._rules.add_rule(rule,**context)
  add_rules = add_rule

  def rule_keys(self):
    '''return list of keys which have associated rules'''
    return self._rules.keys()

  def preload_rules(self,keys=None):
    '''precalculate values defined by rules'''
    if keys is None:
      keys = self.rule_keys()
    for key in keys:
      self[key]
    return self

  def reset_rules(self,keys=None):
    '''remove values previously calculated by rules (will be recalculated on demand)'''
    if keys is None:
      keys = self.rule_keys()
    for key in keys:
      if self._made.has_key(key):
        del self._made[key]
        try:
          del self[key]
        except KeyError:
          pass

  def _get_vector(self,key):
    # try to return existing value
    try:
      # uninitialized vectors will return None
      out = self._data[key]
    except KeyError:
      out = None
    if out is not None:
      return out
    # otherwise try to derive value
    return self._makehook(key)

  def _makehook(self,key):
    '''try to calculate rule-based value'''
    # if no rule, let KeyError raise
    rule = self._rules[key]
    assert not self._made.has_key(key), 'circular dependency for %s' % key
    self._made[key] = 0.0
    
    self._inprog.append(time.time())
    rule(self._rulearg)
    secs = time.time() - self._inprog.pop()
    self._made[key] = secs
    # adjust start time for nested interruptions
    self._inprog = [start+secs for start in self._inprog]

    return self._data[key]

  def get_rule_times(self):
    return self._made

  def str_rule_times(self,keys=None,order='time'):
    out = []
    if keys is None:
      keys = self.rule_keys()
    data = [(self._made.get(key,None),key) for key in keys]
    if order == 'time':
      data.sort(reverse=True)
    for timing,key in data:
      if timing is None:
        extra = '<not run>'
      else:
        extra = '%1.9f secs,  %9.1f ops/sec' % (timing,len(self)/timing)
      out.append('Time: %-16s %s' % (key,extra))
    return '\n'.join(out)

  def _summhook(self):
    out = []
    out.append(self.str_rule_times())
    out.append(repr(self._rules))
    return '\n'.join(out)

  def eval(self,expr,**context):
    '''evaluate an expression in the local unitable namespace'''
    context.setdefault('__',self)
    expr = Expr(expr,**context)
    return expr(self._rulearg)
