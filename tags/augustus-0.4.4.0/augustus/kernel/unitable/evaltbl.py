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

  Rules may call external vector functions.

  >>> from augustus.external import pygsl
  >>> rules.add_rule('e=gaussian_pdf(a,5)',gaussian_pdf=pygsl.rng.gaussian_pdf)
  >>> print x['e']
  [ 0.07365403  0.04839414  0.01579003  0.04839414]

  Parameters to functions may be assigned symbolically
  in the rule definition. 

  >>> rules.add_rule('f=rng.gaussian_pdf(a,x)',rng=pygsl.rng,x=5)
  >>> print x['f']
  [ 0.07365403  0.04839414  0.01579003  0.04839414]

  Rules may be arbitrarily complex so long as vector operations
  are used consistently.

  >>> rules.add_rule('g = ((a/f)*((b+c)**2))%2')
  >>> print x['g']
  [ 0.61591974  1.86455468  1.46800035  0.61386694]

  >>> print rules
  Rule('c=a+b')
  Rule('aa=a.astype("Float")')
  Rule('d=aa/c')
  Rule('e=gaussian_pdf(a,5)', bindings=['gaussian_pdf'])
  Rule('f=rng.gaussian_pdf(a,x)', bindings=['rng', 'x'])
  Rule('g = ((a/f)*((b+c)**2))%2')

  >>> z = EvalTable(rules)
  >>> z['a'] = [2,5,9,5]
  >>> z['b'] = [0,-1,3,5]
  >>> assert z.preload_rules()

  >>> print z.eval('a>b')
  [1 1 1 0]

  Appending or extending the table values cause the rules
  to be applied to the additional values.

  >>> x.append([1,2])
  >>> print x['c']
  [ 2  4 12 10  3]


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

#################################################################
#################################################################

if __name__ == "__main__":
  from augustus.external import pygsl  # needed for this doctest
  import doctest
  flags =  doctest.NORMALIZE_WHITESPACE
  flags |= doctest.ELLIPSIS
  flags |= doctest.REPORT_ONLY_FIRST_FAILURE
  doctest.testmod(optionflags=flags)

#################################################################
