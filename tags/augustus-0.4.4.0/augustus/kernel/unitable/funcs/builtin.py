"""Core set of built-in universal functions, based on numpy

  >>> funcs = StdFuncs()
  >>> a = [1,2,3]
  >>> b = [6,-5,0]
  >>> ab = funcs.run('+',a,b)
  >>> print ab
  [ 7 -3  3]
  >>> c = funcs.run('=',a)
  >>> c is a
  True
  >>> d = funcs.run('copy',a)
  >>> d is a
  False
  >>> d == a
  True
  >>> e = funcs.run('*',a,b,c,d)
  >>> print e
  [  6 -40   0]
  >>>

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


import inspect
from copy import copy
import numpy as na
import os

#from core import Function, Binding

##############################################################################

numpy_ufunc_names = (
    # Binary math
    'add', 'subtract', 'multiply', 'divide', 'remainder',
    'power', 'fmod', 'maximum', 'minimum',
    # Binary logical
    'equal', 'not_equal', 'greater', 'greater_equal', 'less', 'less_equal',
    'logical_and', 'logical_or', 'logical_xor',
    # Unary logical
    'logical_not',
    # Binary bitwise
    'bitwise_and', 'bitwise_or', 'bitwise_xor', 'bitwise_not', 'rshift', 'lshift',
    # Unary math
    'abs', 'fabs', 'ceil', 'floor', 'exp', 'log', 'log10', 'sqrt',
    'arccos', 'arccosh', 'arcsin', 'arcsinh', 'arctan', 'arctanh',
    'cos', 'cosh', 'sin', 'sinh', 'tan', 'tanh',
    # Undocumented
    'floor_divide', 'true_divide', 'hypot', 'ieeemask', 'minus',
  )

ufunc_alias_symbolic = {
    '+'		: 'add',
    '-'		: 'subtract',
    '*'		: 'multiply',
    '/'		: 'divide',
    '//'	: 'true_divide',
    '%'		: 'remainder',
    '**'	: 'power',
    '>'		: 'greater',
    '>='	: 'greater_equal',
    '=='	: 'equal',
    '<'		: 'less',
    '<='	: 'less_equal',
    '!='	: 'not_equal',
    '&'		: 'bitwise_and',
    '&&'	: 'logical_and',
    '!'		: 'logical_not',
    '|'		: 'bitwise_or',
    '||'	: 'logical_or',
    '^'		: 'bitwise_xor',
    '~'		: 'bitwise_not',
    '>>'	: 'rshift',
    '<<'	: 'lshift',
}

ufunc_alias_pythonic = {
    'max'	: 'maximum',
    'min'	: 'minimum',
    'and'	: 'logical_and',
    'or'	: 'logical_or',
    'xor'	: 'logical_xor',
    'not'	: 'logical_not',
}

####################################

class EvalExpr(object):
  def __init__(self,op):
    self.op = op
  def __call__(self,x,y):
    return eval('x %s y' % self.op)
  def __repr__(self):
    return "lambda x,y: x %s y" % self.op

####################################

class Function(object):
  """defines a callable function"""

  _attr_keys = ('nargs','astype','hist')

  def __init__(self,func,**kwargs):
    self.func = func
    self._attr = kwargs

  def get_attr(self): return dict(self._attr)
  attr = property(get_attr)

  def get_nargs(self): return self._attr.get('nargs')
  nargs = property(get_nargs)

  def get_astype(self): return self._attr.get('astype')
  astype = property(get_astype)

  def get_hist(self): return self._attr.get('hist')
  hist = property(get_hist)

  def local_attr(self,**kwargs):
    attr = {}
    attr.update(self._attr)
    attr.update(kwargs)
    return attr

  def __call__(self,*args,**kwargs):
    attr = self.local_attr(**kwargs)
    args = self._prep_callargs(args,**attr)
    nargs = self._prep_nargs(args,**attr)

    # chunk calls if needed
    if len(args) <= nargs:
      out = self._exec_call(*args)
    else:
      while True:
        callargs = args[:nargs]
        args = list(args[nargs:])
        out = self._exec_call(*callargs)
        if not len(args): break
        args.insert(0,out)

    return self._prep_result(out,**attr)

  def _exec_call(self,*args,**kwargs):
    # subclass hook
    out = self.func(*args,**kwargs)
    return out

  def _prep_callargs(self,args,**kwargs):
    # subclass hook
    return args

  def _prep_nargs(self,args,nargs=None,**kwargs):
    if nargs is None: nargs = len(args)
    if nargs <= 1:
      assert len(args) == nargs, 'call requires exactly %s args, got %s, for func: %r' \
                                  % (nargs,len(args),self.func)
    return nargs

  def _prep_result(self,out,astype=None,**kwargs):
    if astype is not None:
      out = out.astype(astype)
    return out


  def _repr_func(self,func):
    import re
    bound = 'unbound'
    if hasattr(self,'args'):
      bound = 'bound'
    out = re.sub(r' at [x0-9a-fA-F]+','',repr(func))
    out = '%s func %s' % (bound,out)
    return out

  def __str__(self):
    func = self._repr_func(self.func)
    out = []
    for key,value in sorted(self._attr.items()):
      if value is not None:
        out.append('%s=%r' % (key,value))
    out = 'func(%s): %s' % (','.join(out),func)
    return out


####################################

class UFuncFunction(Function):
  """wrapper for universal functions"""

  # the following apply to both numbers and strings
  # so they need special handling
  _eval_functions = {
    'greater'		: '>',
    'greater_equal'	: '>=',
    'equal'		: '==',
    'less'		: '<',
    'less_equal'	: '<=',
    'not_equal'		: '!=',
  }

  def __init__(self,func,*args,**kwargs):
    if type(func) == type(''):
      func = self.get_ufunc_callable(func)
    kwargs['nargs'] = self.get_nargs(func)
    Function.__init__(self,func,*args,**kwargs)

  @classmethod
  def get_ufunc_callable(self,name):
    if self._eval_functions.has_key(name):
      op = self._eval_functions[name]
      out = EvalExpr(op)
    else:
      out = getattr(na,name)
    return out

  @staticmethod
  def get_nargs(func,default=None):
    if isinstance(func,na.ufunc._BinaryUFunc):
      return 2
    elif isinstance(func,na.ufunc._UnaryUFunc):
      return 1
    elif isinstance(func,EvalExpr):
      return 2
    else:
      return default

  @staticmethod
  def _repr_func(func):
    out = repr(func)
    out2 = func.__class__.__name__
    if out[1:6] == out2[-5:] == 'UFunc':
      out = out[:1] + out2.lstrip('_') + out[6:]
    return out

####################################

class AddonFuncFactory(object):

  @classmethod
  def __call__(self,funcname,klass=Function):
    func = getattr(self,funcname)
    kwargs = self._get_kwargs(func)
    out = klass(func,**kwargs)
    return out

  @staticmethod
  def _get_kwargs(obj):
    '''return dict of args with default values'''
    (args,varargs,varkw,defaults) = inspect.getargspec(obj)
    if defaults is None:
      return {}
    keys = args[-len(defaults):]
    return dict(zip(keys,defaults))

  @classmethod
  def assign(self,arg,nargs=1): return arg

  @classmethod
  def copy(self,arg,nargs=1): return copy(arg)

####################################

addon_alias = {
    '='		: 'assign',
    'alias'	: 'assign',
}

addon_func_names = (
    'assign', 'copy', 
)

builtin_alias_dicts = (
  addon_alias,
  ufunc_alias_pythonic,
  ufunc_alias_symbolic,
)

builtin_func_factories = (
  (AddonFuncFactory(), addon_func_names),
  (UFuncFunction, numpy_ufunc_names),
)

##############################################################################

class FuncLibBase(dict):
  """for packaging libraries of universal functions
  """
  def __init__(self,*args,**kwargs):
    dict.__init__(self)
    self._update(*args,**kwargs)

  def _update(self,*args,**kwargs):
    for arg in args:
      self.update(arg)
    self.update(**kwargs)

  def trace_alias(self,key):
    seen = []
    while key not in seen:
      seen.append(key)
      value = self[key]
      if type(value) != type(''):
        break
      key = value
    return seen

  def get_func(self,key):
    key = self.trace_alias(key)[-1]
    return self[key]

  def bind_func(self,key,*args,**kwargs):
    func = self.get_func(key)
    return Binding(func,*args,**kwargs)

  def run(self,key,*args,**kwargs):
    func = self.bind_func(key,*args,**kwargs)
    return func(*args)

  def __call__(self,key,*args,**kwargs):
    return self.bind_func(key,*args,**kwargs)
    
  def __str__(self):
    out = []
    for name,value in sorted(self.items()):
      if type(value) == type(''):
        value = 'alias for %r' % value
      else:
        value = str(value)
      out.append('%-15s: %s' % (name,value))
    return ( os.linesep ).join(out)

#################################################################

class StdFuncs(FuncLibBase):
  """Builtin Function Library, suitable for subclassing or updating"""

  _alias_dicts = builtin_alias_dicts
  _func_factories = builtin_func_factories

  def __init__(self,*args,**kwargs):
    # setup builtins first, so caller may override
    FuncLibBase.__init__(self)
    self._add_builtins()
    self._update(*args,**kwargs)

  def _add_builtins(self):
    self._update(*self._alias_dicts)
    for factory,names in self._func_factories:
      for name in names:
        func = factory(name)
        self[name] = func

#################################################################

class StdRules(StdFuncs):
  """TODO: extend for other rule types"""


#################################################################
#################################################################

class Binding(object):
  """Support for objects that can be bound to a computation
  namespace.  Input/Output relationships and the mapping of
  names to the external namespace are recorded.
  """
  def __init__(self,rule,*args,**kwargs):
    self._rule = rule
    self._attr = rule.attr
    self._attr.update(kwargs)
    self.bind_args(*args)

  def __getattr__(self,key):
    return self._attr.get(key)

  def bind_args(self,*fullargs):
    '''provide list of arguments binding to a specific namespace'''
    # fullargs represents args to the callable func
    # - a text value is a variable reference to a named column
    # - any other type is a const value binding to that position
    self._fullargs = list(fullargs)
    # index positions of variable references
    self._vararg_index = self._index_varargs(*fullargs)
    # post list of references that must be provided
    # attribute 'args' is used in the execution context
    self.args = [fullargs[i] for i in self._vararg_index]

  def _index_varargs(self,*fullargs):
    '''return index of variable args (those referenced by name)'''
    out = []
    for i,arg in enumerate(fullargs):
      if type(arg) == type(''):
        out.append(i)
    return out

  def __call__(self,*args,**kwargs):
    callargs = self._prep_callargs(*args) 
    return self._rule(*callargs,**kwargs)

  def _prep_callargs(self,*varargs):
    '''if binding exists, populate full arg list'''
    try:
      fullargs = self._fullargs[:]
      vindex = self._vararg_index
    except AttributeError:
      return varargs
    # replace varargs, keeping constants in place
    for i,arg in zip(vindex,varargs):
      fullargs[i] = arg
    return fullargs

  def __str__(self):
    func = str(self._rule)
    fullargs = ','.join([repr(arg) for arg in self._fullargs])
    varargs = ','.join([repr(arg) for arg in self.args])
    out = []
    out = ['(%s) using %s(%s)' % (varargs,func,fullargs)]
    #if self.nargs is not None:
    #  out.append('max_args_per_call=%s' % self.nargs)
    if self.astype is not None:
      out.append('returns %s' % self.astype)
    if self.nocache:
      out.append('nocache=%s' % self.nocache)
    return ', '.join(out)

##############################################################################
##############################################################################

if __name__ == '__main__':
  import doctest
  doctest.testmod()
  print '________________ Contents of Builtin Rule Library ________________' + os.linesep
  print StdRules()

##############################################################################
