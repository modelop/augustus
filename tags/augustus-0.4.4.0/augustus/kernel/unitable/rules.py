"""UniTable Rules: specification of rules as python expressions
  
  Rules are specified as simple python assignment expressions.
  When evaluated, the local namespace of a rule is the UniTable
  itself.  Variable referenced or assigned refer to unitable fields
  unless otherwise specified as parameters to the rule.

  When used with an EvalTable, fields defined by rules are not
  calculated until/unless referenced.  Dependencies between rules
  are handled automatically on demand.

  Rules may be predefined before loading the table data, or
  added dynamically as needed.

  >>> rules = Rules('c=a+b','aa=a.astype("Float")','d=aa/c')
  >>> print rules
  Rule('c=a+b')
  Rule('aa=a.astype("Float")')
  Rule('d=aa/c')

  Rules may call external vector functions.  Parameters to functions
  may be assigned symbolically in the rule definition. 

  >>> from augustus.external import pygsl
  >>> rules.add_rule('e=gaussian_pdf(a,5)',gaussian_pdf=pygsl.rng.gaussian_pdf)

  >>> rules.add_rule('f=rng.gaussian_pdf(a,x)',rng=pygsl.rng,x=5)

  Rules may be arbitrarily complex so long as vector operations
  are used consistently.

  >>> rules.add_rule('g = ((a/f)*((b+c)**2))%2')

  >>> print rules
  Rule('c=a+b')
  Rule('aa=a.astype("Float")')
  Rule('d=aa/c')
  Rule('e=gaussian_pdf(a,5)', bindings=['gaussian_pdf'])
  Rule('f=rng.gaussian_pdf(a,x)', bindings=['rng', 'x'])
  Rule('g = ((a/f)*((b+c)**2))%2')


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


import symtable
import compiler
import os

#################################################################
#################################################################

class AutoLoader(object):
  '''AutoLoad module objects on demand.

  >>> loader = AutoLoader()
  >>> print type(loader['_'])   #doctest: +ELLIPSIS
  <class '...veclib.base.corelib.VecLib'>

  '''
  _loadmap = {
    '_':	('veclib','veclib',None)
  }
  def __init__(self,**kwargs):
    self.loadmap = self._loadmap.copy()
    self.loadmap.update(kwargs)
    self._cache = {}

  def __contains__(self,key):
    return key in self.loadmap

  def __getitem__(self,key):
    _cache = self._cache
    if key in _cache:
      return _cache[key]
    if key not in self.loadmap:
      return None
    mod,item,attr = self.loadmap[key]
    out = __import__(mod,globals(),None,[item])
    if item:
      out = getattr(out,item)
    if attr:
      out = getattr(out,attr)
    _cache[key] = out
    return out


#################################################################

class HasNamespace(object):
  """Mixin class for objects that have a namespace.  Abstract
  data uses frozenset to ensure no modification at class level.

  """
  all_symbols = frozenset()	# complete symbol set
  sym_assigned = frozenset()	# symbols that are assignment targets
  sym_internal = frozenset()	# symbols both assigned and referenced
  sym_table = {}		# bound symboltable

  mode = 'abstract'		# for use in symtable inspection


  @property
  def sym_exported(self):	# symbols assigned but never referenced
    return self.sym_assigned.difference(self.sym_internal)

  @property
  def sym_referenced(self):	# symbols that are looked up
    return self.all_symbols.difference(self.sym_exported)

  @property
  def sym_bindable(self):	# symbols that allow an external binding
    return self.sym_referenced.difference(self.sym_internal)

  @property
  def sym_bound(self):		# symbols bound to an external object
    return self.sym_bindable.intersection(self.sym_table.keys())

  @property
  def sym_unbound(self):	# symbols that will be referenced on exec
    return self.sym_bindable.difference(self.sym_bound)

  @property
  def sym_visible(self):        # symbols global in scope (no local bindings)
    return self.all_symbols.difference(self.sym_bound)

  @property
  def sym_dump(self,names=('assigned','internal','bound','unbound','exported')):
    return ', '.join([('%s=%s' % (name,sorted(getattr(self,'sym_'+name)))) for name in names])

  def _inspect(self,string):
    symtab = symtable.symtable(string,'rule',self.mode)
    symbols = symtab.get_symbols()
    defs = [sym.get_name() for sym in symbols if sym.is_assigned()]
    refs = [sym.get_name() for sym in symbols if sym.is_referenced()]
    self.all_symbols = frozenset(symtab.get_identifiers())
    self.sym_assigned = defset = frozenset(defs)
    self.sym_internal = frozenset(defset.intersection(refs))

  def __repr__(self):
    return "%s(%r%s)" % (
      self._get_class_str(),
      self._get_body_str(),
      self._get_extra_str(),
    )

  def _get_class_str(self): return self.__class__.__name__
  def _get_body_str(self): return getattr(self,'string','')
  def _get_extra_str(self): return ''

#################################################################

class ExprString(HasNamespace):
  """Represents a single expression defined in a string containing
     python code.  There are no default globals() or bindings
     associated with this object.  It is based purely on what
     can be discovered from the string expression.
     The expression cannot have any assignments.

  >>> r = ExprString('b + c')
  >>> print r.sym_dump
  assigned=[], internal=[], bound=[], unbound=['b', 'c'], exported=[]
  >>> print r(b=2,c=3)
  5
  >>> print r.eval(b=4,c=5)
  9
  >>> print r(b=6)
  Traceback (most recent call last):
  ...
  NameError: name 'c' is not defined

  """
  mode = 'eval'

  def __init__(self,string):
    self._init_string(string)

  def _init_string(self,string):
    if isinstance(string,HasNamespace):
      string = string.string
    self.string = string
    self.code = compiler.compile(string,repr(self),self.mode)
    self._inspect(string)
    assert not self.sym_assigned, 'expression cannot contain assignment: %r' % string

  def __call__(self,locals={},**context):
    _globals = self.sym_table.copy()
    _globals.update(context)
    return eval(self.code,_globals,locals)

  def eval(self,*args,**kwargs):
    return self(*args,**kwargs)

#################################################################

class RuleString(HasNamespace):
  """Represents a single rule defined in a string containing
     python code.  There are no default globals() or bindings
     associated with this object.  It is based purely on what
     can be discovered from the string expression.

  >>> r = RuleString('a = b + c')
  >>> print r.sym_dump
  assigned=['a'], internal=[], bound=[], unbound=['b', 'c'], exported=['a']
  >>> print r(b=2,c=3)
  {'a': 5}
  >>> print r.eval(b=4,c=5)
  9
  >>> print r(b=6)
  Traceback (most recent call last):
  ...
  NameError: name 'c' is not defined

  """
  mode = 'exec'

  def __init__(self,string):
    self._init_string(string)

  def _init_string(self,string):
    if isinstance(string,HasNamespace):
      string = string.string
    self.string = string
    self.code = compiler.compile(string,repr(self),self.mode)
    self._inspect(string)
    if not self.sym_assigned:
      raise SyntaxError, 'rule must contain assignment: %r' % string

  def __call__(self,locals={},**context):
    _globals = self.sym_table.copy()
    _globals.update(context)
    eval(self.code,_globals,locals)
    return locals

  def eval(self,*args,**kwargs):
    return self(*args,**kwargs)[self.name]

  @property
  def name(self):
    '''returns name of variable assigned by this rule,
       or raises exception if not exactly one name is assigned'''
    defs = self.sym_assigned
    if len(defs) > 1:
      raise NameError, 'attempt to reference name of compound rule'
    try:
      return list(defs)[0]
    except:
      raise NameError, 'attempt to reference name of empty rule'


#################################################################
#################################################################

class HasBindings(object):
  """mixin for object with namespace binding support

  """
  def __init__(self,string,*args,**kwargs):
    self.sym_table = sym_table = {}
    if isinstance(string,HasBindings):
      string = string.string
      _globals = dict(string.sym_table)
    else:
      _globals = {}
    self._init_string(string)
    for arg in args:
      _globals.update(arg)
    _globals.update(kwargs)
    autoloader = AutoLoader()
    for key in self.sym_unbound:
      if key in _globals:
        sym_table[key] = _globals[key]
      elif key in autoloader:
        sym_table[key] = autoloader[key]

  def _get_context(self,remove=()):
    killkeys = ['__builtins__']
    if isinstance(remove,basestring):
      killkeys.append(remove)
    else:
      killkeys.extend(remove)
    context = dict(self.sym_table)
    for key in killkeys:
      if context.has_key(key):
        del context[key]
    return context

  def _get_extra_str(self):
    bindings = self._get_context().keys()
    if not bindings:
      return ''
    return ', bindings=%r' % sorted(bindings)

#################################################################

class Expr(HasBindings,ExprString):
  """Expr: container for a ExprString with namespace binding support

  >>> r = Expr('b + c',c=5)
  >>> print r
  Expr('b + c', bindings=['c'])
  >>> print r.sym_dump
  assigned=[], internal=[], bound=['c'], unbound=['b'], exported=[]
  >>> print r(b=2,c=3)
  5
  >>> print r.eval(b=4,c=5)
  9
  >>> print r(b=6)
  11

  >>> b = Expr('_.logical_and(b,c)')
  >>> print b.sym_table  #doctest: +ELLIPSIS
  {'_': <...veclib.base.corelib.VecLib object at ...>}

  >>> error = Expr('a = b + c')
  Traceback (most recent call last):
  ...
  SyntaxError: invalid syntax


  """

#################################################################
#################################################################

class Rule(HasBindings,RuleString):
  """Rule: container for a RuleString with namespace binding support

  >>> r = Rule('a = b + c',c=5)
  >>> print r
  Rule('a = b + c', bindings=['c'])
  >>> print r.sym_dump
  assigned=['a'], internal=[], bound=['c'], unbound=['b'], exported=['a']
  >>> print r(b=2,c=3)
  {'a': 5}
  >>> print r.eval(b=4,c=5)
  9
  >>> print r(b=6)
  {'a': 11}

  >>> b = Rule('a = _.logical_and(b,c)')
  >>> print b.sym_table  #doctest: +ELLIPSIS
  {'_': <...veclib.base.corelib.VecLib object at ...>}

  >>> d = Rule('a = b = c')
  >>> print d.name
  Traceback (most recent call last):
  ...
  NameError: attempt to reference name of compound rule

  >>> error = Rule('b + c')
  Traceback (most recent call last):
  ...
  SyntaxError: rule must contain assignment: 'b + c'

  """

#################################################################

class Rules(HasNamespace):
  """represents a set of rules

    >>> a1 = Rule('a = b + c',c=5)
    >>> a2 = Rule('z = a + c/x',x=2)
    >>> a = Rules(a1,a2)
    >>> print a.sym_dump
    assigned=['a', 'z'], internal=['a'], bound=[], unbound=['b', 'c'], exported=['z']
    >>> print a['z']
    Rule('z = a + c/x', bindings=['x'])

    >>> a3 = Rule('z = a*2 + c/x',x=2)
    >>> b = Rules(a1,a2,a3)
    >>> print b.sym_dump
    assigned=['a', 'z'], internal=['a'], bound=[], unbound=['b', 'c'], exported=['z']
    >>> print b['z']
    Rule('z = a*2 + c/x', bindings=['x'])


  """
  def __init__(self,*args,**context):
    self.rules = []
    self.trigger = {}
    self.all_symbols = set()
    self.sym_assigned = set()
    self.sym_internal = set()
    for arg in args:
      self.add_rule(arg,**context)

  def copy(self):
    return Rules(*self.rules)
    
  def add(self,arg,**kwargs):
    return self.add_rule(arg,**kwargs)

  def add_rule(self,rule,**context):
    if not rule: return
    if isinstance(rule,(list,tuple)):
      for x in rule:
        self.add_rule(x,**context)
      return
    if isinstance(rule,Rules):
      for x in rule.rules:
        self._add_rule(x)
      return
    if not isinstance(rule,Rule):
      rule = Rule(rule,**context)
    self._add_rule(rule)

  def _add_rule(self,rule):
    assert isinstance(rule,Rule)
    self.rules.append(rule)
    for key in rule.sym_assigned:
      self.trigger[key] = rule
    self.all_symbols.update(rule.sym_visible)
    self.sym_assigned.update(rule.sym_assigned)
    self.sym_internal.update(rule.sym_internal)
    self.sym_internal.update(self.sym_assigned.intersection(rule.sym_unbound))
    self.sym_internal.update(self.sym_assigned.intersection(self.sym_unbound))

  def keys(self): return [rule.name for rule in self.rules]
  def has_key(self,key): return key in self.sym_assigned
  def __getitem__(self,key): return self.trigger[key]
  def __iter__(self): return iter(self.rules)

  def __repr__(self):
    out = [repr(rule) for rule in self]
    return (os.linesep).join(out)

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
