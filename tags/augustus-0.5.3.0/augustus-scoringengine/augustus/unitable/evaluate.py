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

import numpy
import re
import math
import itertools

from augustus.core.python3transition import *
from augustus.unitable.utilities import typeToDtype

class ExpressionParsingError(SyntaxError): pass

# operator precedence, tightest to loosest (most of these are implemented by the table)
# ( )            # not in table
# **
# u- ~
# * / // %
# + -
# << >>
# &
# ^
# |
# in not in < <= > >= == != <>
# not
# and
# xor
# or
# ,              # not in table

_SHIFT  = _O = 0   # (codes chosen to look as different as possible on the screen)
_REDUCE = __ = 1
_ACCEPT = _j = 2
_action = \
{    None: {"**": _O, "u-": _O, "~": _O, "*": _O, "/": _O, "//": _O, "%": _O, "+": _O, "-": _O, ">>": _O, "<<": _O, "&": _O, "^": _O, "|": _O, "in": _O, "not in": _O, "<": _O, "<=": _O, ">": _O, ">=": _O, "==": _O, "!=": _O, "<>": _O, "not": _O, "and": _O, "xor": _O, "or": _O, None: _j},
     "**": {"**": _O, "u-": _O, "~": _O, "*": __, "/": __, "//": __, "%": __, "+": __, "-": __, ">>": __, "<<": __, "&": __, "^": __, "|": __, "in": __, "not in": __, "<": __, "<=": __, ">": __, ">=": __, "==": __, "!=": __, "<>": __, "not": __, "and": __, "xor": __, "or": __, None: __},
     "u-": {"**": __, "u-": _O, "~": _O, "*": __, "/": __, "//": __, "%": __, "+": __, "-": __, ">>": __, "<<": __, "&": __, "^": __, "|": __, "in": __, "not in": __, "<": __, "<=": __, ">": __, ">=": __, "==": __, "!=": __, "<>": __, "not": __, "and": __, "xor": __, "or": __, None: __},
      "~": {"**": __, "u-": _O, "~": _O, "*": __, "/": __, "//": __, "%": __, "+": __, "-": __, ">>": __, "<<": __, "&": __, "^": __, "|": __, "in": __, "not in": __, "<": __, "<=": __, ">": __, ">=": __, "==": __, "!=": __, "<>": __, "not": __, "and": __, "xor": __, "or": __, None: __},
      "*": {"**": _O, "u-": _O, "~": _O, "*": __, "/": __, "//": __, "%": __, "+": __, "-": __, ">>": __, "<<": __, "&": __, "^": __, "|": __, "in": __, "not in": __, "<": __, "<=": __, ">": __, ">=": __, "==": __, "!=": __, "<>": __, "not": __, "and": __, "xor": __, "or": __, None: __},
      "/": {"**": _O, "u-": _O, "~": _O, "*": __, "/": __, "//": __, "%": __, "+": __, "-": __, ">>": __, "<<": __, "&": __, "^": __, "|": __, "in": __, "not in": __, "<": __, "<=": __, ">": __, ">=": __, "==": __, "!=": __, "<>": __, "not": __, "and": __, "xor": __, "or": __, None: __},
     "//": {"**": _O, "u-": _O, "~": _O, "*": __, "/": __, "//": __, "%": __, "+": __, "-": __, ">>": __, "<<": __, "&": __, "^": __, "|": __, "in": __, "not in": __, "<": __, "<=": __, ">": __, ">=": __, "==": __, "!=": __, "<>": __, "not": __, "and": __, "xor": __, "or": __, None: __},
      "%": {"**": _O, "u-": _O, "~": _O, "*": __, "/": __, "//": __, "%": __, "+": __, "-": __, ">>": __, "<<": __, "&": __, "^": __, "|": __, "in": __, "not in": __, "<": __, "<=": __, ">": __, ">=": __, "==": __, "!=": __, "<>": __, "not": __, "and": __, "xor": __, "or": __, None: __},
      "+": {"**": _O, "u-": _O, "~": _O, "*": _O, "/": _O, "//": _O, "%": _O, "+": __, "-": __, ">>": __, "<<": __, "&": __, "^": __, "|": __, "in": __, "not in": __, "<": __, "<=": __, ">": __, ">=": __, "==": __, "!=": __, "<>": __, "not": __, "and": __, "xor": __, "or": __, None: __},
      "-": {"**": _O, "u-": _O, "~": _O, "*": _O, "/": _O, "//": _O, "%": _O, "+": __, "-": __, ">>": __, "<<": __, "&": __, "^": __, "|": __, "in": __, "not in": __, "<": __, "<=": __, ">": __, ">=": __, "==": __, "!=": __, "<>": __, "not": __, "and": __, "xor": __, "or": __, None: __},
     ">>": {"**": _O, "u-": _O, "~": _O, "*": _O, "/": _O, "//": _O, "%": _O, "+": _O, "-": _O, ">>": __, "<<": __, "&": __, "^": __, "|": __, "in": __, "not in": __, "<": __, "<=": __, ">": __, ">=": __, "==": __, "!=": __, "<>": __, "not": __, "and": __, "xor": __, "or": __, None: __},
     "<<": {"**": _O, "u-": _O, "~": _O, "*": _O, "/": _O, "//": _O, "%": _O, "+": _O, "-": _O, ">>": __, "<<": __, "&": __, "^": __, "|": __, "in": __, "not in": __, "<": __, "<=": __, ">": __, ">=": __, "==": __, "!=": __, "<>": __, "not": __, "and": __, "xor": __, "or": __, None: __},
      "&": {"**": _O, "u-": _O, "~": _O, "*": _O, "/": _O, "//": _O, "%": _O, "+": _O, "-": _O, ">>": _O, "<<": _O, "&": __, "^": __, "|": __, "in": __, "not in": __, "<": __, "<=": __, ">": __, ">=": __, "==": __, "!=": __, "<>": __, "not": __, "and": __, "xor": __, "or": __, None: __},
      "^": {"**": _O, "u-": _O, "~": _O, "*": _O, "/": _O, "//": _O, "%": _O, "+": _O, "-": _O, ">>": _O, "<<": _O, "&": _O, "^": __, "|": __, "in": __, "not in": __, "<": __, "<=": __, ">": __, ">=": __, "==": __, "!=": __, "<>": __, "not": __, "and": __, "xor": __, "or": __, None: __},
      "|": {"**": _O, "u-": _O, "~": _O, "*": _O, "/": _O, "//": _O, "%": _O, "+": _O, "-": _O, ">>": _O, "<<": _O, "&": _O, "^": _O, "|": __, "in": __, "not in": __, "<": __, "<=": __, ">": __, ">=": __, "==": __, "!=": __, "<>": __, "not": __, "and": __, "xor": __, "or": __, None: __},
     "in": {"**": _O, "u-": _O, "~": _O, "*": _O, "/": _O, "//": _O, "%": _O, "+": _O, "-": _O, ">>": _O, "<<": _O, "&": _O, "^": _O, "|": _O, "in": __, "not in": __, "<": __, "<=": __, ">": __, ">=": __, "==": __, "!=": __, "<>": __, "not": __, "and": __, "xor": __, "or": __, None: __},
 "not in": {"**": _O, "u-": _O, "~": _O, "*": _O, "/": _O, "//": _O, "%": _O, "+": _O, "-": _O, ">>": _O, "<<": _O, "&": _O, "^": _O, "|": _O, "in": __, "not in": __, "<": __, "<=": __, ">": __, ">=": __, "==": __, "!=": __, "<>": __, "not": __, "and": __, "xor": __, "or": __, None: __},
      "<": {"**": _O, "u-": _O, "~": _O, "*": _O, "/": _O, "//": _O, "%": _O, "+": _O, "-": _O, ">>": _O, "<<": _O, "&": _O, "^": _O, "|": _O, "in": __, "not in": __, "<": __, "<=": __, ">": __, ">=": __, "==": __, "!=": __, "<>": __, "not": __, "and": __, "xor": __, "or": __, None: __},
     "<=": {"**": _O, "u-": _O, "~": _O, "*": _O, "/": _O, "//": _O, "%": _O, "+": _O, "-": _O, ">>": _O, "<<": _O, "&": _O, "^": _O, "|": _O, "in": __, "not in": __, "<": __, "<=": __, ">": __, ">=": __, "==": __, "!=": __, "<>": __, "not": __, "and": __, "xor": __, "or": __, None: __},
      ">": {"**": _O, "u-": _O, "~": _O, "*": _O, "/": _O, "//": _O, "%": _O, "+": _O, "-": _O, ">>": _O, "<<": _O, "&": _O, "^": _O, "|": _O, "in": __, "not in": __, "<": __, "<=": __, ">": __, ">=": __, "==": __, "!=": __, "<>": __, "not": __, "and": __, "xor": __, "or": __, None: __},
     ">=": {"**": _O, "u-": _O, "~": _O, "*": _O, "/": _O, "//": _O, "%": _O, "+": _O, "-": _O, ">>": _O, "<<": _O, "&": _O, "^": _O, "|": _O, "in": __, "not in": __, "<": __, "<=": __, ">": __, ">=": __, "==": __, "!=": __, "<>": __, "not": __, "and": __, "xor": __, "or": __, None: __},
     "==": {"**": _O, "u-": _O, "~": _O, "*": _O, "/": _O, "//": _O, "%": _O, "+": _O, "-": _O, ">>": _O, "<<": _O, "&": _O, "^": _O, "|": _O, "in": __, "not in": __, "<": __, "<=": __, ">": __, ">=": __, "==": __, "!=": __, "<>": __, "not": __, "and": __, "xor": __, "or": __, None: __},
     "!=": {"**": _O, "u-": _O, "~": _O, "*": _O, "/": _O, "//": _O, "%": _O, "+": _O, "-": _O, ">>": _O, "<<": _O, "&": _O, "^": _O, "|": _O, "in": __, "not in": __, "<": __, "<=": __, ">": __, ">=": __, "==": __, "!=": __, "<>": __, "not": __, "and": __, "xor": __, "or": __, None: __},
     "<>": {"**": _O, "u-": _O, "~": _O, "*": _O, "/": _O, "//": _O, "%": _O, "+": _O, "-": _O, ">>": _O, "<<": _O, "&": _O, "^": _O, "|": _O, "in": __, "not in": __, "<": __, "<=": __, ">": __, ">=": __, "==": __, "!=": __, "<>": __, "not": __, "and": __, "xor": __, "or": __, None: __},
    "not": {"**": __, "u-": __, "~": __, "*": __, "/": __, "//": __, "%": __, "+": __, "-": __, ">>": __, "<<": __, "&": __, "^": __, "|": __, "in": __, "not in": __, "<": __, "<=": __, ">": __, ">=": __, "==": __, "!=": __, "<>": __, "not": _O, "and": __, "xor": __, "or": __, None: __},
    "and": {"**": _O, "u-": _O, "~": _O, "*": _O, "/": _O, "//": _O, "%": _O, "+": _O, "-": _O, ">>": _O, "<<": _O, "&": _O, "^": _O, "|": _O, "in": _O, "not in": _O, "<": _O, "<=": _O, ">": _O, ">=": _O, "==": _O, "!=": _O, "<>": _O, "not": _O, "and": __, "xor": __, "or": __, None: __},
    "xor": {"**": _O, "u-": _O, "~": _O, "*": _O, "/": _O, "//": _O, "%": _O, "+": _O, "-": _O, ">>": _O, "<<": _O, "&": _O, "^": _O, "|": _O, "in": _O, "not in": _O, "<": _O, "<=": _O, ">": _O, ">=": _O, "==": _O, "!=": _O, "<>": _O, "not": _O, "and": _O, "xor": __, "or": __, None: __},
     "or": {"**": _O, "u-": _O, "~": _O, "*": _O, "/": _O, "//": _O, "%": _O, "+": _O, "-": _O, ">>": _O, "<<": _O, "&": _O, "^": _O, "|": _O, "in": _O, "not in": _O, "<": _O, "<=": _O, ">": _O, ">=": _O, "==": _O, "!=": _O, "<>": _O, "not": _O, "and": _O, "xor": _O, "or": __, None: __},
}

# translation from operator characters to function names

_binary = {"**": "power",
           "*": "multiply",
           "/": "true_divide",
           "//": "floor_divide",
           "%": "fmod",
           "+": "add",
           "-": "subtract",
           "<<": "left_shift",
           ">>": "right_shift",
           "&": "bitwise_and",
           "^": "bitwise_xor",
           "|": "bitwise_or",
           "in": "isin",
           "not in": "notin",
           "<": "less",
           "<=": "less_equal",
           ">": "greater",
           ">=": "greater_equal",
           "==": "equal",
           "!=": "not_equal",
           "<>": "not_equal",
           "and": "logical_and",
           "xor": "logical_xor",
           "or": "logical_or",
           }

_unary = {"u-": "negative",
           "~": "invert",
           "not": "logical_not",
          }

_comparisons = set(("less", "less_equal", "greater", "greater_equal", "equal", "not_equal"))

# expression tree representation

class Expression(object):
    empty = " "
    bool = "?"
    int = "l"
    float = "d"
    str = "S"
    set = "{"
    _typeName = {"?": "bool", "l": "int", "d": "float", "S": "str", "{": "set"}

    def simplify(self): return self
    def checkArgs(self): pass
    def checkTypes(self): pass
    def checkSets(self, constant=False, flat=False, homogeneous=False, streamline=False):
        if streamline and (not constant or not flat or not homogeneous):
            raise TypeError("streamline can only be used if the sets are required to be constant, flat, and homogeneous")
    def findFields(self, fields): pass

class FunctionExpression(Expression):
    function = {"add": numpy.add,
                "subtract": numpy.subtract,
                "multiply": numpy.multiply,
                "divide": numpy.divide,
                "true_divide": numpy.true_divide,
                "floor_divide": numpy.floor_divide,
                "negative": numpy.negative,
                "power": numpy.power,
                "remainder": numpy.remainder,
                "mod": numpy.mod,
                "fmod": numpy.fmod,
                "absolute": numpy.absolute,
                "abs": numpy.abs,
                "rint": numpy.rint,
                "round": None,
                "sign": numpy.sign,
                "conj": numpy.conj,
                "exp": numpy.exp,
                "log": numpy.log,
                "log2": numpy.log2,
                "log10": numpy.log10,
                "expm1": numpy.expm1,
                "log1p": numpy.log1p,
                "sqrt": numpy.sqrt,
                "square": numpy.square,
                "sqr": numpy.square,
                "reciprocal": numpy.reciprocal,
                "sin": numpy.sin,
                "cos": numpy.cos,
                "tan": numpy.tan,
                "arcsin": numpy.arcsin,
                "asin": numpy.arcsin,
                "arccos": numpy.arccos,
                "acos": numpy.arccos,
                "arctan": numpy.arctan,
                "atan": numpy.arctan,
                "arctan2": numpy.arctan2,
                "atan2": numpy.arctan2,
                "hypot": numpy.hypot,
                "sinh": numpy.sinh,
                "cosh": numpy.cosh,
                "tanh": numpy.tanh,
                "arcsinh": numpy.arcsinh,
                "asinh": numpy.arcsinh,
                "arccosh": numpy.arccosh,
                "acosh": numpy.arccosh,
                "arctanh": numpy.arctanh,
                "atanh": numpy.arctanh,
                "bitwise_and": numpy.bitwise_and,
                "bitwise_or": numpy.bitwise_or,
                "bitwise_xor": numpy.bitwise_xor,
                "invert": numpy.invert,
                "bitwise_not": numpy.bitwise_not,
                "left_shift": numpy.left_shift,
                "right_shift": numpy.right_shift,
                "greater": numpy.greater,
                "greater_equal": numpy.greater_equal,
                "less": numpy.less,
                "less_equal": numpy.less_equal,
                "not_equal": None,
                "equal": None,
                "logical_and": numpy.logical_and,
                "logical_or": numpy.logical_or,
                "logical_xor": numpy.logical_xor,
                "logical_not": numpy.logical_not,
                "and": numpy.logical_and,
                "or": numpy.logical_or,
                "xor": numpy.logical_xor,
                "not": numpy.logical_not,
                "maximum": numpy.maximum,
                "minimum": numpy.minimum,
                "isfinite": numpy.isfinite,
                "isinf": numpy.isinf,
                "isnan": numpy.isnan,
                "signbit": numpy.signbit,
                "ldexp": numpy.ldexp,
                "floor": numpy.floor,
                "ceil": numpy.ceil,
                "isin": None,
                "notin": None,
                }

    unary = set(("negative", "absolute", "abs", "rint", "sign", "conj",
                 "exp", "exp2", "log", "log2", "log10", "expm1", "log1p",
                 "sqrt", "square", "sqr", "reciprocal",
                 "sin", "cos", "tan", "arcsin", "asin", "arccos", "acos", "arctan", "atan",
                 "sinh", "cosh", "tanh", "arcsinh", "asinh", "arccosh", "acosh", "arctanh", "atanh",
                 "deg2rad", "rad2deg"
                 "invert", "bitwise_not",
                 "isfinite", "isinf", "isnan",
                 "signbit", "floor", "ceil", "trunc",
                 ))

    binary = set(("add", "subtract", "multiply", "divide", "true_divide", "floor_divide",
                  "logaddexp", "logaddexp2",
                  "power", "remainder", "mod", "fmod",
                  "arctan2", "atan2", "hypot",
                  "bitwise_and", "bitwise_or", "bitwise_xor",
                  "left_shift", "right_shift",
                  "greater", "greater_equal", "less", "less_equal", "not_equal", "equal",
                  "logical_and", "logical_or", "logical_xor", "logical_not", "and", "or", "xor", "not",
                  "maximum", "minimum",
                  "copysign", "nextafter",
                  "ldexp",
                  "isin", "notin",
                  ))

    def __init__(self, name, *args, **kwds):
        if name not in self.function:
            raise ValueError("unrecognized function name \"%s\"" % name)
        self.name = name
        self.func = self.function[name]
        self.args = list(args)
        self.__dict__.update(kwds)

    def __repr__(self):
        return "%s(%s)" % (self.name, ", ".join(map(repr, self.args)))

    def __hash__(self):
        return hash((self.name, tuple(self.args)))

    def findFields(self, fields):
        for x in self.args:
            x.findFields(fields)

    def simplify(self):
        self.args = [x.simplify() for x in self.args]

        if len(self.args) == 2 and isinstance(self.args[0], ConstantExpression) and isinstance(self.args[1], ConstantExpression):
            if self.name == "add":
                return self.args[0] + self.args[1]
            elif self.name == "subtract":
                return self.args[0] - self.args[1]
            elif self.name == "multiply":
                return self.args[0] * self.args[1]
            elif self.name == "divide" or self.name == "true_divide":
                return self.args[0] / self.args[1]
            elif self.name == "floor_divide":
                return self.args[0] // self.args[1]
            elif self.name == "logaddexp":
                return self.args[0].logaddexp(self.args[1])
            elif self.name == "logaddexp2":
                return self.args[0].logaddexp2(self.args[1])
            elif self.name == "power":
                return self.args[0] ** self.args[1]
            elif self.name == "remainder" or self.name == "mod" or self.name == "fmod":
                return self.args[0] % self.args[1]
            elif self.name == "round":
                return self.args[0].round(self.args[1])
            elif self.name == "arctan2" or self.name == "atan2":
                return self.args[0].arctan2(self.args[1])
            elif self.name == "hypot":
                return self.args[0].hypot(self.args[1])
            elif self.name == "bitwise_and":
                return self.args[0] & self.args[1]
            elif self.name == "bitwise_or":
                return self.args[0] | self.args[1]
            elif self.name == "bitwise_xor":
                return self.args[0] ^ self.args[1]
            elif self.name == "left_shift":
                return self.args[0] << self.args[1]
            elif self.name == "right_shift":
                return self.args[0] >> self.args[1]
            elif self.name == "greater":
                return self.args[0] > self.args[1]
            elif self.name == "greater_equal":
                return self.args[0] >= self.args[1]
            elif self.name == "less":
                return self.args[0] < self.args[1]
            elif self.name == "less_equal":
                return self.args[0] <= self.args[1]
            elif self.name == "not_equal":
                return self.args[0] != self.args[1]
            elif self.name == "equal":
                return self.args[0] == self.args[1]
            elif self.name == "logical_and" or self.name == "and":
                return self.args[0].logical_and(self.args[1])
            elif self.name == "logical_or" or self.name == "or":
                return self.args[0].logical_or(self.args[1])
            elif self.name == "logical_xor" or self.name == "xor":
                return self.args[0].logical_xor(self.args[1])
            elif self.name == "maximum":
                return self.args[0].maximum(self.args[1])
            elif self.name == "minimum":
                return self.args[0].minimum(self.args[1])
            elif self.name == "ldexp":
                return self.args[0].ldexp(self.args[1])
            elif self.name == "isin":
                return self.args[0].isin(self.args[1])
            elif self.name == "notin":
                return self.args[0].notin(self.args[1])

        elif len(self.args) == 1 and isinstance(self.args[0], ConstantExpression):
            if self.name == "negative":
                return -self.args[0]
            elif self.name == "absolute" or self.name == "abs":
                return abs(self.args[0])
            elif self.name == "rint" or self.name == "round":
                return self.args[0].round()
            elif self.name == "sign":
                return self.args[0].sign()
            elif self.name == "exp":
                return self.args[0].exp()
            elif self.name == "exp2":
                return self.args[0].exp2()
            elif self.name == "log":
                return self.args[0].log()
            elif self.name == "log2":
                return self.args[0].log2()
            elif self.name == "log10":
                return self.args[0].log10()
            elif self.name == "expm1":
                return self.args[0].expm1()
            elif self.name == "log1p":
                return self.args[0].log1p()
            elif self.name == "sqrt":
                return self.args[0].sqrt()
            elif self.name == "square" or self.name == "sqr":
                return self.args[0].square()
            elif self.name == "reciprocal":
                return self.args[0].reciprocal()
            elif self.name == "sin":
                return self.args[0].sin()
            elif self.name == "cos":
                return self.args[0].cos()
            elif self.name == "tan":
                return self.args[0].tan()
            elif self.name == "arcsin" or self.name == "asin":
                return self.args[0].arcsin()
            elif self.name == "arccos" or self.name == "acos":
                return self.args[0].arccos()
            elif self.name == "arctan" or self.name == "atan":
                return self.args[0].arctan()
            elif self.name == "sinh":
                return self.args[0].sinh()
            elif self.name == "cosh":
                return self.args[0].cosh()
            elif self.name == "tanh":
                return self.args[0].tanh()
            elif self.name == "arcsinh" or self.name == "asinh":
                return self.args[0].arcsinh()
            elif self.name == "arccosh" or self.name == "acosh":
                return self.args[0].arccosh()
            elif self.name == "arctanh" or self.name == "atanh":
                return self.args[0].arctanh()
            elif self.name == "deg2rad":
                return self.args[0].deg2rad()
            elif self.name == "rad2deg":
                return self.args[0].rad2deg()
            elif self.name == "invert" or self.name == "bitwise_not":
                return ~self.args[0]
            elif self.name == "logical_not" or self.name == "not":
                return self.args[0].logical_not()
            elif self.name == "isfinite":
                return self.args[0].isfinite()
            elif self.name == "isinf":
                return self.args[0].isinf()
            elif self.name == "isnan":
                return self.args[0].isnan()
            elif self.name == "signbit":
                return self.args[0].signbit()
            elif self.name == "floor":
                return self.args[0].floor()
            elif self.name == "ceil":
                return self.args[0].ceil()
            elif self.name == "trunc":
                return self.args[0].trunc()

        elif self.name == "isin" and len(self.args) == 2 and isinstance(self.args[1], ConstantExpression) and self.args[1].type[0] == self.set and len(self.args[1].value) == 0:
            return ConstantExpression(False, Expression.bool, hash=hash(False))
        
        elif self.name == "notin" and len(self.args) == 2 and isinstance(self.args[1], ConstantExpression) and self.args[1].type[0] == self.set and len(self.args[1].value) == 0:
            return ConstantExpression(True, Expression.bool, hash=hash(True))

        return self

    def checkArgs(self):
        for x in self.args: x.checkArgs()

        if self.name in self.unary:
            if len(self.args) != 1:
                raise ExpressionParsingError("number of arguments to '%s' must be 1, not %d" % (self.name, len(self.args)))

        elif self.name in self.binary:
            if len(self.args) != 2:
                raise ExpressionParsingError("number of arguments to '%s' must be 2, not %d" % (self.name, len(self.args)))

        if self.name == "round":
            if len(self.args) == 0 or len(self.args) > 2:
                raise ExpressionParsingError("number of arguments to 'round' must be 1 or 2, not %d" % len(self.args))
            if len(self.args) == 2 and not isinstance(self.args[1], ConstantExpression):
                raise ExpressionParsingError("second argument of 'round' must be a constant, not \"%s\"" % repr(self.args[1]))

        if self.name == "isin" or self.name == "notin":
            if len(self.args) != 2:
                raise ExpressionParsingError("number of arguments to '%s' must be 2, not %d" % (self.name, len(self.args)))
            if not isinstance(self.args[1], ConstantExpression) or self.args[1].type != Expression.set:
                raise ExpressionParsingError("second argument of '%s' must be a constant set, not \"%s\"" % (self.name, repr(self.args[1])))

    def checkTypes(self):
        argTypes = [x.checkTypes() for x in self.args]
        solution = None

        if self.func is not None:
            for inputType, outputType in [x.split("->") for x in self.func.types]:
                if len(inputType) == 1:
                    if len(argTypes) == 1 and numpy.can_cast(argTypes[0], inputType):
                        if outputType != "O":
                            solution = outputType
                            break

                elif len(inputType) == 2:
                    if len(argTypes) == 2 and numpy.can_cast(argTypes[0], inputType[0]) and numpy.can_cast(argTypes[1], inputType[1]):
                        if outputType != "O":
                            solution = outputType
                            break

        elif self.name == "round":
            if len(argTypes) == 1 and numpy.can_cast(argTypes[0], "d"):
                solution = "d"
            elif len(argTypes) == 2 and numpy.can_cast(argTypes[0], "d") and numpy.can_cast(argTypes[1], "l"):
                solution = "d"

        elif self.name == "equal" or self.name == "not_equal":
            for inputType, outputType in [x.split("->") for x in (numpy.equal.types + ["SS->?"])]:
                if numpy.can_cast(argTypes[0], inputType[0]) and numpy.can_cast(argTypes[1], inputType[1]):
                    if outputType != "O":
                        solution = outputType
                        break

        elif self.name == "isin" or self.name == "notin":
            if argTypes[1][0] == self.set:
                for inputType, outputType in [x.split("->") for x in (numpy.equal.types + ["SS->?"])]:
                    if numpy.can_cast(argTypes[0], inputType[0]) and numpy.can_cast(argTypes[1][1], inputType[1]):
                        if outputType != "O":
                            solution = outputType
                            break

        else:
            raise NotImplementedError()

        if solution is None:
            raise ExpressionParsingError("cannot evaluate %s because arguments are: %s" % (repr(self), ", ".join([self._typeName[x[0]] for x in argTypes])))

        self.type = solution
        return solution

    def checkSets(self, constant=False, flat=False, homogeneous=False, streamline=False):
        for x in self.args: x.checkSets(constant=constant, flat=flat, homogeneous=homogeneous, streamline=streamline)

    def _calculate_binary(self, arrays, lookups, func):
        arg0, arg1 = self.args[0].calculate(arrays, lookups), self.args[1].calculate(arrays, lookups)
        if not isinstance(self.args[0], (FieldExpression, ConstantExpression)) and self.args[0].type == self.type:
            func(arg0, arg1, arg0)
            return arg0
        elif not isinstance(self.args[1], (FieldExpression, ConstantExpression)) and self.args[1].type == self.type:
            func(arg0, arg1, arg1)
            return arg1
        else:
            return func(arg0, arg1)

    def calculate(self, arrays, lookups):
        if self.name == "round":
            arg = self.args[0].calculate(arrays, lookups)
            if len(self.args) == 1:
                return numpy.round(arg)
            else:
                arg1 = self.args[1].calculate(arrays, lookups)
                return numpy.round(arg, arg1)

        elif self.name == "equal" or self.name == "not_equal":
            if self.args[0].type == self.str or self.args[1].type == self.str:
                if isinstance(self.args[0], FieldExpression) and self.args[0].name in lookups and isinstance(self.args[1], ConstantExpression):
                    if not hasattr(self.args[1], "_lookup_value"):
                        v_to_n = lookups[self.args[0].name][0]
                        self.args[1]._lookup_value = v_to_n.get(self.args[1].value, None)
                    if self.name == "equal":
                        return numpy.equal(arrays[self.args[0].name], self.args[1]._lookup_value)
                    else:
                        return numpy.not_equal(arrays[self.args[0].name], self.args[1]._lookup_value)

                if isinstance(self.args[1], FieldExpression) and self.args[1].name in lookups and isinstance(self.args[0], ConstantExpression):
                    if not hasattr(self.args[0], "_lookup_value"):
                        v_to_n = lookups[self.args[1].name][0]
                        self.args[0]._lookup_value = v_to_n.get(self.args[0].value, None)
                    if self.name == "equal":
                        return numpy.equal(arrays[self.args[1].name], self.args[0]._lookup_value)
                    else:
                        return numpy.not_equal(arrays[self.args[1].name], self.args[0]._lookup_value)

                left = self.args[0].calculate(arrays, lookups)
                if isinstance(self.args[0], FieldExpression) and self.args[0].name in lookups:
                    n_to_v = lookups[self.args[0].name][1]
                    values = numpy.array([n_to_v[j] for j in xrange(len(n_to_v))])
                    left = values[left]

                right = self.args[1].calculate(arrays, lookups)
                if isinstance(self.args[1], FieldExpression) and self.args[1].name in lookups:
                    n_to_v = lookups[self.args[1].name][1]
                    values = numpy.array([n_to_v[j] for j in xrange(len(n_to_v))])
                    right = values[right]

                if isinstance(left, (list, tuple, numpy.ndarray)) and isinstance(right, (list, tuple, numpy.ndarray)):
                    if self.name == "equal":
                        return numpy.fromiter((l == r for l, r in itertools.izip(left, right)), dtype=numpy.bool)
                    else:
                        return numpy.fromiter((l != r for l, r in itertools.izip(left, right)), dtype=numpy.bool)
                elif isinstance(left, (list, tuple, numpy.ndarray)):
                    if self.name == "equal":
                        return numpy.fromiter((l == right for l in left), dtype=numpy.bool)
                    else:
                        return numpy.fromiter((l != right for l in left), dtype=numpy.bool)
                elif isinstance(right, (list, tuple, numpy.ndarray)):
                    if self.name == "equal":
                        return numpy.fromiter((left == r for r in right), dtype=numpy.bool)
                    else:
                        return numpy.fromiter((left != r for r in right), dtype=numpy.bool)
                else:
                    assert False, "equal constants should have been evaluated already"

            else:
                if self.name == "equal":
                    return self._calculate_binary(arrays, lookups, numpy.equal)
                else:
                    return self._calculate_binary(arrays, lookups, numpy.not_equal)

        elif self.name == "isin" or self.name == "notin":
            output = None
            if self.args[0].type == self.str:
                if isinstance(self.args[0], FieldExpression) and self.args[0].name in lookups:
                    if not hasattr(self.args[1], "_lookup_value"):
                        v_to_n = lookups[self.args[0].name][0]
                        self.args[1]._lookup_value = numpy.fromiter((v_to_n[v] for v in self.args[1].value), dtype=typeToDtype["category"])
                    output = numpy.in1d(self.args[0].calculate(arrays, lookups), self.args[1]._lookup_value)

            if output is None:
                output = numpy.in1d(self.args[0].calculate(arrays, lookups), self.args[1].value)

            if self.name == "isin":
                return output
            else:
                numpy.logical_not(output, output)
                return output

        elif self.name in self.unary:
            arg = self.args[0].calculate(arrays, lookups)
            if not isinstance(self.args[0], (FieldExpression, ConstantExpression)) and self.args[0].type == self.type:
                self.func(arg, arg)
                return arg
            else:
                return self.func(arg)

        elif self.name in self.binary:
            return self._calculate_binary(arrays, lookups, self.func)

        else:
            raise NotImplementedError()

for x in "logaddexp", "logaddexp2", "exp2", "deg2rad", "rad2deg", "copysign", "nextafter", "trunc":
    try:
        FunctionExpression.function[x] = eval("numpy." + x)
    except AttributeError:
        def notImplemented(*args, **kwds):
            raise NotImplementedError("function \"%s\" is not available in your version of NumPy (%s)" % (x, numpy.__version__))
        FunctionExpression.function[x] = notImplemented

class FieldExpression(Expression):
    typeMapper = {"object": None, "category": Expression.str, "string": Expression.str,
                  "integer": Expression.int, "float": Expression.float, "double": Expression.float,
                  "<i": Expression.int, "<i16": Expression.int, "<i4": Expression.int, "<i8": Expression.int, ">i": Expression.int, ">i16": Expression.int, ">i4": Expression.int, ">i8": Expression.int, "i": Expression.int, "i16": Expression.int, "i4": Expression.int, "i8": Expression.int, "int16": Expression.int, "int32": Expression.int, "int64": Expression.int, "int8": Expression.int,
                  "<u": Expression.int, "<u16": Expression.int, "<u4": Expression.int, "<u8": Expression.int, ">u": Expression.int, ">u16": Expression.int, ">u4": Expression.int, ">u8": Expression.int, "u": Expression.int, "u16": Expression.int, "u4": Expression.int, "u8": Expression.int,
                  "<f": Expression.float, "<f16": Expression.float, "<f4": Expression.float, "<f8": Expression.float, ">f": Expression.float, ">f16": Expression.float, ">f4": Expression.float, ">f8": Expression.float, "f": Expression.float, "f16": Expression.float, "f4": Expression.float, "f8": Expression.float, "float128": Expression.float, "float16": Expression.float, "float32": Expression.float, "float64": Expression.float}

    def __init__(self, name, type, **kwds):
        self.name = name
        self.type = self.typeMapper.get(type, None)

        if self.type is None:
            raise NotImplementedError("field \"%s\" has type \"%s\", which cannot be used in calculations" % (name, type))

        self.__dict__.update(kwds)

    def __hash__(self):
        return hash(self.name)

    def __repr__(self):
        return self.name

    def findFields(self, fields):
        fields.add(self.name)

    def calculate(self, arrays, lookups):
        return arrays[self.name]

    def checkTypes(self):
        return self.type

class ConstantExpression(Expression):
    def __init__(self, value, type, **kwds):
        self.value = value
        self.type = type
        self.__dict__.update(kwds)

    def __repr__(self):
        if self.type == self.set:
            return "{%s}" % (", ".join(map(repr, self.value)))
        else:
            return repr(self.value) # + {self.bool: "B", self.int: "I", self.float: "F", self.str: "S", self.set: ""}[self.type]

    def __hash__(self):
        return self.hash

    def simplify(self):
        if self.type == self.set:
            self.value = set([x.simplify() for x in self.value])
            self.hash = hash(tuple(sorted(self.value, lambda a, b: cmp(hash(a), hash(b)))))
        return self

    def checkArgs(self):
        if self.type == self.set:
            for x in self.value:
                x.checkArgs()

    def checkTypes(self):
        if hasattr(self, "subtype"):
            return self.type + self.subtype
        else:
            return self.type

    def checkSets(self, constant=False, flat=False, homogeneous=False, streamline=False):
        if self.type == self.set:
            if homogeneous: subtypes = set()
            for x in self.value:
                if constant and isinstance(x, (FunctionExpression, FieldExpression)):
                    raise ExpressionParsingError("set contains a non-constant \"%s\"" % repr(x))

                elif isinstance(x, ConstantExpression):
                    if flat and x.type == self.set:
                        raise ExpressionParsingError("set is not flat")
                    x.checkSets(constant=constant, flat=flat, homogeneous=homogeneous, streamline=streamline)

                    if homogeneous:
                        if numpy.can_cast(x.type, self.int):
                            subtypes.add(self.int)
                        elif numpy.can_cast(x.type, self.float):
                            subtypes.add(self.float)
                        else:
                            subtypes.add(x.type)

                else:
                    raise TypeError("Is it possible to get to this point in the code? %s %s", (repr(self.value), repr(type(self.value))))

            if homogeneous:
                if subtypes == set((self.int, self.float)):
                    self.subtype = self.float

                elif len(subtypes) == 1:
                    self.subtype = subtypes.pop()

                elif len(subtypes) == 0:
                    self.subtype = self.empty

                else:
                    raise ExpressionParsingError("set is not homogeneous (types are {%s})" % ", ".join(map(lambda x: self._typeName[x], subtypes)))

            if streamline:
                Expression.checkSets(self, constant=constant, flat=flat, homogeneous=homogeneous, streamline=streamline)
                self.value = numpy.array([v.value for v in self.value])

    def calculate(self, arrays, lookups):
        return self.value

    def _combine(self, other, operation):
        if isinstance(other, ConstantExpression):
            kwds = dict(self.__dict__)
            del kwds["value"]
            del kwds["type"]

            if operation == "__lt__":
                result = self.value < other.value
            elif operation == "__le__":
                result = self.value <= other.value
            elif operation == "__eq__":
                result = self.value == other.value
            elif operation == "__ne__":
                result = self.value != other.value
            elif operation == "__gt__":
                result = self.value > other.value
            elif operation == "__ge__":
                result = self.value >= other.value
            elif operation == "isin":
                result = self in other.value
            elif operation == "notin":
                result = self not in other.value
            elif callable(operation):
                result = operation(self.value, other.value)
            else:
                result = getattr(self.value, operation, lambda x: NotImplemented)(other.value)
                if result == NotImplemented and operation[0:2] == "__":
                    operation = "__r" + operation[2:]
                    result = getattr(other.value, operation, lambda x: NotImplemented)(self.value)

            if result is True or result is False:
                theType = Expression.bool
            elif isinstance(result, int):
                theType = Expression.int
            elif isinstance(result, float):
                theType = Expression.float
            elif isinstance(result, basestring):
                theType = Expression.str
            elif isinstance(result, set):
                theType = Expression.set
            else:
                raise TypeError("unsupported operand types for %s: 'ConstantExpression.%s' and 'ConstantExpression.%s'" % (operation, self._typeName[self.type], self._typeName[other.type]))

            kwds["hash"] = hash(result)
            return ConstantExpression(result, theType, **kwds)
        else:
            if operation == "__eq__": return False
            if operation == "__ne__": return True
            raise TypeError("unsupported operand types for %s: 'ConstantExpression' and '%s'" % (operation, repr(type(other))))

    def _apply(self, operation):
        kwds = dict(self.__dict__)
        del kwds["value"]
        del kwds["type"]

        if callable(operation):
            result = operation(self.value)
        else:
            result = getattr(self.value, operation, lambda: NotImplemented)()

        if result is True or result is False:
            theType = Expression.bool
        elif isinstance(result, int):
            theType = Expression.int
        elif isinstance(result, float):
            theType = Expression.float
        elif isinstance(result, basestring):
            theType = Expression.str
        elif isinstance(result, set):
            theType = Expression.set
        else:
            raise TypeError("unsupported operand type for %s: 'ConstantExpression.%s'" % (operation, self._typeName[self.type]))

        kwds["hash"] = hash(result)
        return ConstantExpression(result, theType, **kwds)

    def __add__(self, other): return self._combine(other, "__add__")
    def __sub__(self, other): return self._combine(other, "__sub__")
    def __mul__(self, other): return self._combine(other, "__mul__")
    def __div__(self, other): return self._combine(other, "__truediv__")
    def __truediv__(self, other): return self._combine(other, "__truediv__")
    def __floordiv__(self, other): return self._combine(other, "__floordiv__")
    def __mod__(self, other): return self._combine(other, "__mod__")
    def __pow__(self, other): return self._combine(other, "__pow__")
    def __lshift__(self, other): return self._combine(other, "__lshift__")
    def __rshift__(self, other): return self._combine(other, "__rshift__")
    def __and__(self, other): return self._combine(other, "__and__")
    def __xor__(self, other): return self._combine(other, "__xor__")
    def __or__(self, other): return self._combine(other, "__or__")
    def __neg__(self): return self._apply("__neg__")
    def __pos__(self): return self._apply("__pos__")
    def __abs__(self): return self._apply("__abs__")
    def __invert__(self): return self._apply("__invert__")

    def __lt__(self, other): return self._combine(other, "__lt__")
    def __le__(self, other): return self._combine(other, "__le__")
    def __eq__(self, other): return self._combine(other, "__eq__")
    def __ne__(self, other): return self._combine(other, "__ne__")
    def __gt__(self, other): return self._combine(other, "__gt__")
    def __ge__(self, other): return self._combine(other, "__ge__")
    def logical_and(self, other): return self._combine(other, lambda x, y: x and y)
    def logical_or(self, other): return self._combine(other, lambda x, y: x or y)
    def logical_xor(self, other): return self._combine(other, lambda x, y: (x and not y) or (not x and y))
    def logical_not(self): return self._apply(lambda x: not x)
    def logaddexp(self, other): return self._combine(other, lambda x, y: math.log(math.exp(x) + math.exp(y)))
    def logaddexp2(self, other): return self._combine(other, lambda x, y: math.log(2**x + 2**y, 2))
    def round(self, other=None):
        if other is None:
            return self._apply(round)
        else:
            return self._combine(other, round)
    def sign(self): return self._apply(lambda x: type(x)(1) if x > 0 else type(x)(-1) if x < 0 else type(x)(0))
    def exp(self): return self._apply(math.exp)
    def exp2(self): return self._apply(lambda x: 2**x)
    def log(self): return self._apply(math.log)
    def log2(self): return self._apply(lambda x: math.log(x, 2))
    def log10(self): return self._apply(lambda x: math.log(x, 10))
    def expm1(self): return self._apply(lambda x: math.exp(x) - 1)
    def log1p(self): return self._apply(lambda x: math.log(1 + x))
    def sqrt(self): return self._apply(math.sqrt)
    def square(self): return self._apply(lambda x: x**2)
    def reciprocal(self): return self._apply(lambda x: 1./x)
    def sin(self): return self._apply(math.sin)
    def cos(self): return self._apply(math.cos)
    def tan(self): return self._apply(math.tan)
    def arcsin(self): return self._apply(math.asin)
    def arccos(self): return self._apply(math.acos)
    def arctan(self): return self._apply(math.atan)
    def arctan2(self, other): return self._combine(other, math.atan2)
    def hypot(self, other): return self._combine(other, lambda x, y: math.sqrt(x**2 + y**2))
    def sinh(self): return self._apply(math.sinh)
    def cosh(self): return self._apply(math.cosh)
    def tanh(self): return self._apply(math.tanh)
    def arcsinh(self): return self._apply(math.asinh)
    def arccosh(self): return self._apply(math.acosh)
    def arctanh(self): return self._apply(math.atanh)
    def deg2rad(self): return self._apply(lambda x: x*math.pi/180.)
    def rad2deg(self): return self._apply(lambda x: x*180./math.pi)
    def maximum(self, other): return self._combine(other, max)
    def minimum(self, other): return self._combine(other, min)
    def isfinite(self): return self._apply(lambda x: not math.isnan(x) and not math.isinf(x))
    def isinf(self): return self._apply(math.isinf)
    def isnan(self): return self._apply(math.isnan)
    def signbit(self): return self._apply(lambda x: x < 0)
    def copysign(self, other): return self._combine(other, lambda x, y: (type(x)(1) if x > 0 else type(x)(-1) if x < 0 else type(x)(0)) * y)
    def ldexp(self, other): return self._combine(other, lambda x, y: x * 2**y)
    def floor(self): return self._apply(math.floor)
    def ceil(self): return self._apply(math.ceil)
    def trunc(self): return self._apply(lambda x: math.floor(x) if x > 0 else math.ceil(x))
    def isin(self, other): return self._combine(other, "isin")
    def notin(self, other): return self._combine(other, "notin")

# parsing functions (user interface: "parse(expression, fields, types)")

_re_number = re.compile("^\s*([-+]?[0-9]*\.?[0-9]+([eE][-+]?[0-9]+)?)\s*", re.UNICODE)
_re_dquote_string = re.compile("^\s*\"([^\"\\\\]*(\\\\.[^\"\\\\]*)*)\"\s*", re.UNICODE)
_re_dquote = re.compile("\\\\\"", re.UNICODE)
_re_squote_string = re.compile("^\s*'([^'\\\\]*(\\\\.[^'\\\\]*)*)'\s*", re.UNICODE)
_re_squote = re.compile("\\\\'", re.UNICODE)
_re_word = re.compile("^\s*([.\w]+)\s*", re.UNICODE)
_re_operator = re.compile("^\s*(\+|-|\*\*|\*|//|/|%|&|\||\^|~|and\s|xor\s|or\s|not\s+in\s|in\s|not\s|<>|>>|<<|>=|>|<=|<|==|!=|\{|\}|\(|\)|,)\s*", re.UNICODE)

def _gettoken(expression, fields, types):
    m = re.search(_re_operator, expression)
    if m is not None:
        return m.group(1).rstrip(), expression[m.span(1)[1]:]

    m = re.search(_re_number, expression)
    if m is not None:
        text = m.group(1)
        try:
            asfloat = float(text)
        except ValueError:
            pass
        else:
            try:
                asint = int(text)
            except ValueError:
                return ConstantExpression(asfloat, Expression.float, hash=hash(asfloat)), expression[m.span(1)[1]:]
            else:
                return ConstantExpression(asint, Expression.int, hash=hash(asint)), expression[m.span(1)[1]:]

    m = re.search(_re_dquote_string, expression)
    if m is not None:
        asstr = re.sub(_re_dquote, "\"", m.group(1))
        return ConstantExpression(asstr, Expression.str, hash=hash(asstr)), expression[m.span(1)[1] + 1:]

    m = re.search(_re_squote_string, expression)
    if m is not None:
        asstr = re.sub(_re_squote, "'", m.group(1))
        return ConstantExpression(asstr, Expression.str, hash=hash(asstr)), expression[m.span(1)[1] + 1:]
    
    m = re.search(_re_word, expression)
    if m is not None:
        text = m.group(1)
        try:
            asexpr = FunctionExpression(text)
        except ValueError:
            if text in fields:
                return FieldExpression(text, types[text]), expression[m.span(1)[1]:]
            else:
                raise ExpressionParsingError("unrecognized token \"%s\"" % text)
        else:
            return asexpr, expression[m.span(1)[1]:]

    return None, ""

def _reduce(top, valstack, oprstack, depth):
    if top in _binary:
        if len(valstack) < 2:
            if len(valstack) < 1:
                raise ExpressionParsingError("binary operation \"%s\" has no arguments" % top)
            else:
                raise ExpressionParsingError("binary operation \"%s\" is missing its second argument" % top)
        b, a = valstack.pop(), valstack.pop()

        lhs = a
        while hasattr(lhs, "chain") and lhs.depth == depth: lhs = lhs.args[1]
        
        if _binary[top] in _comparisons and lhs.depth == depth and isinstance(lhs, FunctionExpression) and lhs.name in _comparisons:
            expr = FunctionExpression("and", a, FunctionExpression(_binary[top], lhs.args[1], b, depth=depth, count=0), depth=depth, count=0)
            expr.chain = True
            valstack.append(expr)
        else:
            valstack.append(FunctionExpression(_binary[top], a, b, depth=depth, count=0))

    elif top in _unary:
        if len(valstack) < 1:
            raise ExpressionParsingError("unary operation \"%s\" is missing an argument" % top)
        a = valstack.pop()
        valstack.append(FunctionExpression(_unary[top], a, depth=depth, count=0))

    oprstack.pop()

    return valstack, oprstack

def parse(expression, fields, types, level="top", depth=0):
    valstack = []
    oprstack = [None]

    token, remaining = _gettoken(expression, fields, types)
    prevtoken = None
    while True:
        # handle special case of unary minus sign
        if token == "-" and prevtoken != "(" and not isinstance(prevtoken, Expression):
            token = "u-"

        if isinstance(token, (ConstantExpression, FieldExpression)):
            token.depth = depth
            token.count = 0
            valstack.append(token)
            prevtoken = token
            token, remaining = _gettoken(remaining, fields, types)

            if isinstance(token, (ConstantExpression, FieldExpression)):
                raise ExpressionParsingError("cannot concatenate expressions %s and %s" % (prevtoken, token))

        else:
            if prevtoken == "," and token in _binary: raise ExpressionParsingError("binary operator \"%s\" cannot come right after a \",\"" % token)

            if token == "{":
                value, remaining = parse(remaining, fields, types, level="{}", depth=depth+1)

                value.sort(lambda a, b: cmp(hash(a), hash(b)))
                valstack.append(ConstantExpression(set(value), Expression.set, hash=hash(tuple(value)), depth=depth, count=0))

                prevtoken = token
                token, remaining = _gettoken(remaining, fields, types)

            elif token == "}":
                if level != "{}": raise ExpressionParsingError("unopened curly bracket")

                while len(oprstack) > 1: valstack, oprstack = _reduce(oprstack[-1], valstack, oprstack, depth)
                return valstack, remaining

            elif token == "(":
                value, remaining = parse(remaining, fields, types, level="()", depth=depth+1)

                if len(value) != 1: raise ExpressionParsingError("parenthetical phrase contains commas (and is not an argument list)")
                valstack.append(value[0])

                prevtoken = token
                token, remaining = _gettoken(remaining, fields, types)

            elif token == ")":
                if level != "()": raise ExpressionParsingError("unopened parenthesis")
                if prevtoken == ",": raise ExpressionParsingError("parenthetical phrase cannot end with \",\"")

                while len(oprstack) > 1: valstack, oprstack = _reduce(oprstack[-1], valstack, oprstack, depth)
                return valstack, remaining

            elif token == ",":
                if len(oprstack) == 1 and len(valstack) == 0:
                    raise ExpressionParsingError("expression may not begin with \",\"")

                while len(oprstack) > 1: valstack, oprstack = _reduce(oprstack[-1], valstack, oprstack, depth)
                prevtoken = token
                token, remaining = _gettoken(remaining, fields, types)

            elif isinstance(token, FunctionExpression):
                prevtoken = token
                token, remaining = _gettoken(remaining, fields, types)
                if token != "(": raise ExpressionParsingError("function name must be followed by \"(\"")

                value, remaining = parse(remaining, fields, types, level="()", depth=depth+1)
                prevtoken.args = value
                prevtoken.depth = depth
                prevtoken.count = 0
                valstack.append(prevtoken)

                prevtoken = token
                token, remaining = _gettoken(remaining, fields, types)

            else:
                top = oprstack[-1]
                action = _action[top][token]

                if action == _SHIFT:
                    oprstack.append(token)
                    prevtoken = token
                    token, remaining = _gettoken(remaining, fields, types)

                elif action == _REDUCE:
                    valstack, oprstack = _reduce(top, valstack, oprstack, depth)

                elif action == _ACCEPT:
                    if level == "()": raise ExpressionParsingError("unclosed parenthesis")
                    if level == "{}": raise ExpressionParsingError("unclosed curly bracket")
                    if len(valstack) == 0: raise ExpressionParsingError("empty expression")

                    for v in valstack: v.checkArgs()
                    valstack = [v.simplify() for v in valstack]
                    for v in valstack: v.checkSets(constant=True, flat=True, homogeneous=True, streamline=True)
                    for v in valstack: v.checkTypes()
                    
                    return valstack

                else:
                    raise ExpressionParsingError(action)
