#!/usr/bin/env python

# Copyright (C) 2006-2013  Open Data ("Open Data" refers to
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

"""This module defines the Formula class."""

import re

from augustus.core.defs import defs
from augustus.core.NumpyInterface import NP
from augustus.core.PmmlExpression import PmmlExpression
from augustus.core.FakeFieldType import FakeFieldType
from augustus.core.DataColumn import DataColumn
from augustus.core.FieldCastMethods import FieldCastMethods

class Formula(PmmlExpression):
    """Formula is an expression that parses a text-based formula to
    generate a tree of PMML expressions.

    It is not a part of standard PMML, though it can expand to strict
    PMML expressions.

    The syntax is mostly a subset of Python, but with a few SQL-like
    operators:
      - Normal operators + - * / < > == != ( ) have their normal
        meanings (with floating-point division, even for integers).
      - Python operators **, //, %, in, not in, and, or, xor, not
        have their Python meanings, including Pythonic features such
        as C{3 <= x <= 5} to mean C{(3 <= x) and (x <= 5)}.
      - SQL operators = (test equality), like, between, not between
        have SQL-like meanings ("like" invokes a Python regular
        expression); C{x between 3 and 5} means an inclusive range.
      - Numbers are interpreted as constants, [ ] as arrays for in
        and not in operators, strings followed by ( as function
        names, and other strings as field references.
      - The quantity must be strictly an expression; no loops,
        conditionals, or assignments; it must expand to valid PMML.
    """

    xsd = """<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema">
    <xs:element name="Formula">
        <xs:complexType mixed="true">
            <xs:sequence>
                <xs:element ref="Extension" minOccurs="0" maxOccurs="unbounded" />
            </xs:sequence>
            <xs:attribute name="mapMissingTo" type="xs:string"/>
            <xs:attribute name="invalidValueTreatment" type="INVALID-VALUE-TREATMENT-METHOD" default="returnInvalid" />
        </xs:complexType>
    </xs:element>
</xs:schema>
"""

    class List(object):
        """A list of PMML <Constants> used by "isIn" and "isNotIn"."""

        def __init__(self):
            self.values = []
            self.fieldType = None

        def addArgument(self, argument):
            self.values.append(argument)

        def __repr__(self):
            return repr(self.values)

    class Constant(object):
        """Equivalent of a PMML <Constant> element."""

        def __init__(self, dataType, value):
            self.fieldType = FakeFieldType(dataType, "continuous")
            self.value = value

        def evaluate(self, dataTable, functionTable, performanceTable):
            data = NP("empty", len(dataTable), dtype=self.fieldType.dtype)
            data[:] = self.value
            return self.fieldType.toDataColumn(data, None)

        def __repr__(self):
            return repr(self.value)

        def asPmml(self, E):
            return E.Constant(str(self.value), dataType=self.fieldType.dataType)

    class FieldRef(object):
        """Equivalent of a PMML <FieldRef> element."""

        def __init__(self, name):
            self.name = name

        def evaluate(self, dataTable, functionTable, performanceTable):
            return dataTable.fields[self.name]

        def __repr__(self):
            return self.name

        def asPmml(self, E):
            return E.FieldRef(field=self.name)

    class Apply(object):
        """Equivalent of a PMML <Apply> element."""

        def __init__(self, function):
            self.function = function
            self.arguments = []

        def addArgument(self, argument):
            if argument.__class__.__name__ == "List":
                self.arguments.extend(argument.values)

            elif self.function in ("between", "notBetween") and isinstance(argument, self.__class__) and argument.function == "and":
                self.arguments.extend(argument.arguments)

            else:
                self.arguments.append(argument)

        def addArguments(self, arguments):
            for argument in arguments:
                self.addArgument(argument)

        def evaluate(self, dataTable, functionTable, performanceTable):
            function = functionTable.get(self.function)
            if function is None:
                raise LookupError("Apply references function \"%s\", but it does not exist" % self.function)

            return function.evaluate(dataTable, functionTable, performanceTable, self.arguments)
            
        def __repr__(self):
            return "%s(%s)" % (self.function, ", ".join(map(repr, self.arguments)))

        def asPmml(self, E):
            return E.Apply(*(x.asPmml(E) for x in self.arguments), function=self.function)

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
    {    None: {"**": _O, "u-": _O, "~": _O, "*": _O, "/": _O, "//": _O, "%": _O, "+": _O, "-": _O, ">>": _O, "<<": _O, "&": _O, "^": _O, "|": _O, "in": _O, "not in": _O, "like": _O, "between": _O, "not between": _O, "<": _O, "<=": _O, ">": _O, ">=": _O, "==": _O, "=": _O, "!=": _O, "<>": _O, "not": _O, "and": _O, "xor": _O, "or": _O, None: _j},
         "**": {"**": _O, "u-": _O, "~": _O, "*": __, "/": __, "//": __, "%": __, "+": __, "-": __, ">>": __, "<<": __, "&": __, "^": __, "|": __, "in": __, "not in": __, "like": __, "between": __, "not between": __, "<": __, "<=": __, ">": __, ">=": __, "==": __, "=": __, "!=": __, "<>": __, "not": __, "and": __, "xor": __, "or": __, None: __},
         "u-": {"**": __, "u-": _O, "~": _O, "*": __, "/": __, "//": __, "%": __, "+": __, "-": __, ">>": __, "<<": __, "&": __, "^": __, "|": __, "in": __, "not in": __, "like": __, "between": __, "not between": __, "<": __, "<=": __, ">": __, ">=": __, "==": __, "=": __, "!=": __, "<>": __, "not": __, "and": __, "xor": __, "or": __, None: __},
          "~": {"**": __, "u-": _O, "~": _O, "*": __, "/": __, "//": __, "%": __, "+": __, "-": __, ">>": __, "<<": __, "&": __, "^": __, "|": __, "in": __, "not in": __, "like": __, "between": __, "not between": __, "<": __, "<=": __, ">": __, ">=": __, "==": __, "=": __, "!=": __, "<>": __, "not": __, "and": __, "xor": __, "or": __, None: __},
          "*": {"**": _O, "u-": _O, "~": _O, "*": __, "/": __, "//": __, "%": __, "+": __, "-": __, ">>": __, "<<": __, "&": __, "^": __, "|": __, "in": __, "not in": __, "like": __, "between": __, "not between": __, "<": __, "<=": __, ">": __, ">=": __, "==": __, "=": __, "!=": __, "<>": __, "not": __, "and": __, "xor": __, "or": __, None: __},
          "/": {"**": _O, "u-": _O, "~": _O, "*": __, "/": __, "//": __, "%": __, "+": __, "-": __, ">>": __, "<<": __, "&": __, "^": __, "|": __, "in": __, "not in": __, "like": __, "between": __, "not between": __, "<": __, "<=": __, ">": __, ">=": __, "==": __, "=": __, "!=": __, "<>": __, "not": __, "and": __, "xor": __, "or": __, None: __},
         "//": {"**": _O, "u-": _O, "~": _O, "*": __, "/": __, "//": __, "%": __, "+": __, "-": __, ">>": __, "<<": __, "&": __, "^": __, "|": __, "in": __, "not in": __, "like": __, "between": __, "not between": __, "<": __, "<=": __, ">": __, ">=": __, "==": __, "=": __, "!=": __, "<>": __, "not": __, "and": __, "xor": __, "or": __, None: __},
          "%": {"**": _O, "u-": _O, "~": _O, "*": __, "/": __, "//": __, "%": __, "+": __, "-": __, ">>": __, "<<": __, "&": __, "^": __, "|": __, "in": __, "not in": __, "like": __, "between": __, "not between": __, "<": __, "<=": __, ">": __, ">=": __, "==": __, "=": __, "!=": __, "<>": __, "not": __, "and": __, "xor": __, "or": __, None: __},
          "+": {"**": _O, "u-": _O, "~": _O, "*": _O, "/": _O, "//": _O, "%": _O, "+": __, "-": __, ">>": __, "<<": __, "&": __, "^": __, "|": __, "in": __, "not in": __, "like": __, "between": __, "not between": __, "<": __, "<=": __, ">": __, ">=": __, "==": __, "=": __, "!=": __, "<>": __, "not": __, "and": __, "xor": __, "or": __, None: __},
          "-": {"**": _O, "u-": _O, "~": _O, "*": _O, "/": _O, "//": _O, "%": _O, "+": __, "-": __, ">>": __, "<<": __, "&": __, "^": __, "|": __, "in": __, "not in": __, "like": __, "between": __, "not between": __, "<": __, "<=": __, ">": __, ">=": __, "==": __, "=": __, "!=": __, "<>": __, "not": __, "and": __, "xor": __, "or": __, None: __},
         ">>": {"**": _O, "u-": _O, "~": _O, "*": _O, "/": _O, "//": _O, "%": _O, "+": _O, "-": _O, ">>": __, "<<": __, "&": __, "^": __, "|": __, "in": __, "not in": __, "like": __, "between": __, "not between": __, "<": __, "<=": __, ">": __, ">=": __, "==": __, "=": __, "!=": __, "<>": __, "not": __, "and": __, "xor": __, "or": __, None: __},
         "<<": {"**": _O, "u-": _O, "~": _O, "*": _O, "/": _O, "//": _O, "%": _O, "+": _O, "-": _O, ">>": __, "<<": __, "&": __, "^": __, "|": __, "in": __, "not in": __, "like": __, "between": __, "not between": __, "<": __, "<=": __, ">": __, ">=": __, "==": __, "=": __, "!=": __, "<>": __, "not": __, "and": __, "xor": __, "or": __, None: __},
          "&": {"**": _O, "u-": _O, "~": _O, "*": _O, "/": _O, "//": _O, "%": _O, "+": _O, "-": _O, ">>": _O, "<<": _O, "&": __, "^": __, "|": __, "in": __, "not in": __, "like": __, "between": __, "not between": __, "<": __, "<=": __, ">": __, ">=": __, "==": __, "=": __, "!=": __, "<>": __, "not": __, "and": __, "xor": __, "or": __, None: __},
          "^": {"**": _O, "u-": _O, "~": _O, "*": _O, "/": _O, "//": _O, "%": _O, "+": _O, "-": _O, ">>": _O, "<<": _O, "&": _O, "^": __, "|": __, "in": __, "not in": __, "like": __, "between": __, "not between": __, "<": __, "<=": __, ">": __, ">=": __, "==": __, "=": __, "!=": __, "<>": __, "not": __, "and": __, "xor": __, "or": __, None: __},
          "|": {"**": _O, "u-": _O, "~": _O, "*": _O, "/": _O, "//": _O, "%": _O, "+": _O, "-": _O, ">>": _O, "<<": _O, "&": _O, "^": _O, "|": __, "in": __, "not in": __, "like": __, "between": __, "not between": __, "<": __, "<=": __, ">": __, ">=": __, "==": __, "=": __, "!=": __, "<>": __, "not": __, "and": __, "xor": __, "or": __, None: __},
         "in": {"**": _O, "u-": _O, "~": _O, "*": _O, "/": _O, "//": _O, "%": _O, "+": _O, "-": _O, ">>": _O, "<<": _O, "&": _O, "^": _O, "|": _O, "in": __, "not in": __, "like": __, "between": __, "not between": __, "<": __, "<=": __, ">": __, ">=": __, "==": __, "=": __, "!=": __, "<>": __, "not": __, "and": __, "xor": __, "or": __, None: __},
     "not in": {"**": _O, "u-": _O, "~": _O, "*": _O, "/": _O, "//": _O, "%": _O, "+": _O, "-": _O, ">>": _O, "<<": _O, "&": _O, "^": _O, "|": _O, "in": __, "not in": __, "like": __, "between": __, "not between": __, "<": __, "<=": __, ">": __, ">=": __, "==": __, "=": __, "!=": __, "<>": __, "not": __, "and": __, "xor": __, "or": __, None: __},
       "like": {"**": _O, "u-": _O, "~": _O, "*": _O, "/": _O, "//": _O, "%": _O, "+": _O, "-": _O, ">>": _O, "<<": _O, "&": _O, "^": _O, "|": _O, "in": __, "not in": __, "like": __, "between": __, "not between": __, "<": __, "<=": __, ">": __, ">=": __, "==": __, "=": __, "!=": __, "<>": __, "not": __, "and": __, "xor": __, "or": __, None: __},
    "between": {"**": _O, "u-": _O, "~": _O, "*": _O, "/": _O, "//": _O, "%": _O, "+": _O, "-": _O, ">>": _O, "<<": _O, "&": _O, "^": _O, "|": _O, "in": __, "not in": __, "like": __, "between": __, "not between": __, "<": __, "<=": __, ">": __, ">=": __, "==": __, "=": __, "!=": __, "<>": __, "not": __, "and": _O, "xor": __, "or": __, None: __},
"not between": {"**": _O, "u-": _O, "~": _O, "*": _O, "/": _O, "//": _O, "%": _O, "+": _O, "-": _O, ">>": _O, "<<": _O, "&": _O, "^": _O, "|": _O, "in": __, "not in": __, "like": __, "between": __, "not between": __, "<": __, "<=": __, ">": __, ">=": __, "==": __, "=": __, "!=": __, "<>": __, "not": __, "and": _O, "xor": __, "or": __, None: __},
          "<": {"**": _O, "u-": _O, "~": _O, "*": _O, "/": _O, "//": _O, "%": _O, "+": _O, "-": _O, ">>": _O, "<<": _O, "&": _O, "^": _O, "|": _O, "in": __, "not in": __, "like": __, "between": __, "not between": __, "<": __, "<=": __, ">": __, ">=": __, "==": __, "=": __, "!=": __, "<>": __, "not": __, "and": __, "xor": __, "or": __, None: __},
         "<=": {"**": _O, "u-": _O, "~": _O, "*": _O, "/": _O, "//": _O, "%": _O, "+": _O, "-": _O, ">>": _O, "<<": _O, "&": _O, "^": _O, "|": _O, "in": __, "not in": __, "like": __, "between": __, "not between": __, "<": __, "<=": __, ">": __, ">=": __, "==": __, "=": __, "!=": __, "<>": __, "not": __, "and": __, "xor": __, "or": __, None: __},
          ">": {"**": _O, "u-": _O, "~": _O, "*": _O, "/": _O, "//": _O, "%": _O, "+": _O, "-": _O, ">>": _O, "<<": _O, "&": _O, "^": _O, "|": _O, "in": __, "not in": __, "like": __, "between": __, "not between": __, "<": __, "<=": __, ">": __, ">=": __, "==": __, "=": __, "!=": __, "<>": __, "not": __, "and": __, "xor": __, "or": __, None: __},
         ">=": {"**": _O, "u-": _O, "~": _O, "*": _O, "/": _O, "//": _O, "%": _O, "+": _O, "-": _O, ">>": _O, "<<": _O, "&": _O, "^": _O, "|": _O, "in": __, "not in": __, "like": __, "between": __, "not between": __, "<": __, "<=": __, ">": __, ">=": __, "==": __, "=": __, "!=": __, "<>": __, "not": __, "and": __, "xor": __, "or": __, None: __},
         "==": {"**": _O, "u-": _O, "~": _O, "*": _O, "/": _O, "//": _O, "%": _O, "+": _O, "-": _O, ">>": _O, "<<": _O, "&": _O, "^": _O, "|": _O, "in": __, "not in": __, "like": __, "between": __, "not between": __, "<": __, "<=": __, ">": __, ">=": __, "==": __, "=": __, "!=": __, "<>": __, "not": __, "and": __, "xor": __, "or": __, None: __},
          "=": {"**": _O, "u-": _O, "~": _O, "*": _O, "/": _O, "//": _O, "%": _O, "+": _O, "-": _O, ">>": _O, "<<": _O, "&": _O, "^": _O, "|": _O, "in": __, "not in": __, "like": __, "between": __, "not between": __, "<": __, "<=": __, ">": __, ">=": __, "==": __, "=": __, "!=": __, "<>": __, "not": __, "and": __, "xor": __, "or": __, None: __},
         "!=": {"**": _O, "u-": _O, "~": _O, "*": _O, "/": _O, "//": _O, "%": _O, "+": _O, "-": _O, ">>": _O, "<<": _O, "&": _O, "^": _O, "|": _O, "in": __, "not in": __, "like": __, "between": __, "not between": __, "<": __, "<=": __, ">": __, ">=": __, "==": __, "=": __, "!=": __, "<>": __, "not": __, "and": __, "xor": __, "or": __, None: __},
         "<>": {"**": _O, "u-": _O, "~": _O, "*": _O, "/": _O, "//": _O, "%": _O, "+": _O, "-": _O, ">>": _O, "<<": _O, "&": _O, "^": _O, "|": _O, "in": __, "not in": __, "like": __, "between": __, "not between": __, "<": __, "<=": __, ">": __, ">=": __, "==": __, "=": __, "!=": __, "<>": __, "not": __, "and": __, "xor": __, "or": __, None: __},
        "not": {"**": __, "u-": __, "~": __, "*": __, "/": __, "//": __, "%": __, "+": __, "-": __, ">>": __, "<<": __, "&": __, "^": __, "|": __, "in": __, "not in": __, "like": __, "between": __, "not between": __, "<": __, "<=": __, ">": __, ">=": __, "==": __, "=": __, "!=": __, "<>": __, "not": _O, "and": __, "xor": __, "or": __, None: __},
        "and": {"**": _O, "u-": _O, "~": _O, "*": _O, "/": _O, "//": _O, "%": _O, "+": _O, "-": _O, ">>": _O, "<<": _O, "&": _O, "^": _O, "|": _O, "in": _O, "not in": _O, "like": _O, "between": _O, "not between": _O, "<": _O, "<=": _O, ">": _O, ">=": _O, "==": _O, "=": _O, "!=": _O, "<>": _O, "not": _O, "and": __, "xor": __, "or": __, None: __},
        "xor": {"**": _O, "u-": _O, "~": _O, "*": _O, "/": _O, "//": _O, "%": _O, "+": _O, "-": _O, ">>": _O, "<<": _O, "&": _O, "^": _O, "|": _O, "in": _O, "not in": _O, "like": _O, "between": _O, "not between": _O, "<": _O, "<=": _O, ">": _O, ">=": _O, "==": _O, "=": _O, "!=": _O, "<>": _O, "not": _O, "and": _O, "xor": __, "or": __, None: __},
         "or": {"**": _O, "u-": _O, "~": _O, "*": _O, "/": _O, "//": _O, "%": _O, "+": _O, "-": _O, ">>": _O, "<<": _O, "&": _O, "^": _O, "|": _O, "in": _O, "not in": _O, "like": _O, "between": _O, "not between": _O, "<": _O, "<=": _O, ">": _O, ">=": _O, "==": _O, "=": _O, "!=": _O, "<>": _O, "not": _O, "and": _O, "xor": _O, "or": __, None: __},
    }

    # translation from operator characters to Apply function names

    # FIXME: these should be names of Apply functions (the ones that are still capitalized are not PMML 4.1 Apply functions)
    _binary = {"**": "pow",
               "*": "*",
               "/": "/",
               "//": "//",
               "%": "mod",
               "+": "+",
               "-": "-",
               "<<": "<<",
               ">>": ">>",
               "&": "&",
               "^": "^",
               "|": "|",
               "in": "isIn",
               "not in": "isNotIn",
               "between": "between",
               "not between": "notBetween",
               "like": "like",
               "<": "lessThan",
               "<=": "lessOrEqual",
               ">": "greaterThan",
               ">=": "greaterOrEqual",
               "==": "equal",
               "=": "equal",
               "!=": "notEqual",
               "<>": "notEqual",
               "and": "and",
               "xor": "xor",
               "or": "or",
               }

    _unary = {"u-": "negative",
               "~": "~",
               "not": "not",
              }

    _comparisons = ("lessThan", "lessOrEqual", "greaterThan", "greaterOrEqual", "equal", "notEqual")

    _re_number = re.compile("^\s*([-+]?([0-9]+\.?[0-9]*|[0-9]*\.?[0-9]+)([eE][-+]?[0-9]+)?)\s*", re.UNICODE)
    _re_dquote_string = re.compile("^\s*\"([^\"\\\\]*(\\\\.[^\"\\\\]*)*)\"\s*", re.UNICODE)
    _re_dquote = re.compile("\\\\\"", re.UNICODE)
    _re_squote_string = re.compile("^\s*'([^'\\\\]*(\\\\.[^'\\\\]*)*)'\s*", re.UNICODE)
    _re_squote = re.compile("\\\\'", re.UNICODE)
    _re_word = re.compile("^\s*([.\w]+)\s*", re.UNICODE)
    _re_operator = re.compile("^\s*(\+|-|\*\*|\*|//|/|%|&|\||\^|~|and\s|xor\s|or\s|not\s+in\s|in\s|not\s+between\s|between\s|not\s|like\s|AND\s|XOR\s|OR\s|NOT\s+IN\s|IN\s|NOT\s+BETWEEN\s|BETWEEN\s|NOT\s|LIKE\s|<>|>>|<<|>=|>|<=|<|==|=|!=|\[|\]|\(|\)|,)\s*", re.UNICODE)
    _re_openparen = re.compile("^\s*\(")

    @classmethod
    def _gettoken(cls, expression):
        """Used by parse."""

        m = re.search(cls._re_operator, expression)
        if m is not None:
            return m.group(1).rstrip().lower(), expression[m.span(1)[1]:]

        m = re.search(cls._re_number, expression)
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
                    return Formula.Constant("double", asfloat), expression[m.span(1)[1]:]
                else:
                    return Formula.Constant("integer", asint), expression[m.span(1)[1]:]

        m = re.search(cls._re_dquote_string, expression)
        if m is not None:
            asstr = re.sub(cls._re_dquote, "\"", m.group(1))
            return Formula.Constant("string", asstr), expression[m.span(1)[1] + 1:]

        m = re.search(cls._re_squote_string, expression)
        if m is not None:
            asstr = re.sub(cls._re_squote, "'", m.group(1))
            return Formula.Constant("string", asstr), expression[m.span(1)[1] + 1:]

        m = re.search(cls._re_word, expression)
        if m is not None:
            text = m.group(1)
            if text == ".":
                raise defs.FormulaParsingError("dot by itcls is not a valid token")

            if re.search(cls._re_openparen, expression[m.span(1)[1]:]) is None:
                return Formula.FieldRef(text), expression[m.span(1)[1]:]
            else:
                return Formula.Apply(text), expression[m.span(1)[1]:]

        return None, ""

    @classmethod
    def _reduce(cls, top, valstack, oprstack, depth):
        """Used by parse."""

        if top in cls._binary:
            if len(valstack) < 2:
                if len(valstack) < 1:
                    raise defs.FormulaParsingError("binary operation \"%s\" has no arguments" % top)
                else:
                    raise defs.FormulaParsingError("binary operation \"%s\" is missing its second argument" % top)
            b, a = valstack.pop(), valstack.pop()

            lhs = a
            while hasattr(lhs, "_chain") and lhs._depth == depth:
                lhs = lhs.arguments[1]

            if cls._binary[top] in cls._comparisons and lhs._depth == depth and isinstance(lhs, Formula.Apply) and lhs.function in cls._comparisons:
                partialexpr = Formula.Apply(cls._binary[top])
                partialexpr.addArgument(lhs.arguments[1])
                partialexpr.addArgument(b)

                partialexpr._depth = depth
                partialexpr._count = 0

                expr = Formula.Apply("and")
                expr.addArgument(a)
                expr.addArgument(partialexpr)
                expr._depth = depth
                expr._count = 0
                expr._chain = True

            else:
                expr = Formula.Apply(cls._binary[top])
                expr.addArgument(a)
                expr.addArgument(b)
                expr._depth = depth
                expr._count = 0

        elif top in cls._unary:
            if len(valstack) < 1:
                raise defs.FormulaParsingError("unary operation \"%s\" is missing an argument" % top)
            a = valstack.pop()

            if top == "u-" and isinstance(a, Formula.Constant):
                expr = Formula.Constant(a.fieldType.dataType, -a.value)
                expr._depth = depth
                expr._count = 0

            else:
                expr = Formula.Apply(cls._unary[top])
                expr.addArgument(a)
                expr._depth = depth
                expr._count = 0

        valstack.append(expr)
        oprstack.pop()
        return valstack, oprstack

    @classmethod
    def _parse(cls, expression, level="top", depth=0):
        """Used by parse."""

        valstack = []
        oprstack = [None]

        token, remaining = cls._gettoken(expression)
        prevtoken = None
        while True:
            # handle special case of unary minus sign
            if token == "-" and prevtoken != "(" and not isinstance(prevtoken, (Formula.List, Formula.Constant, Formula.FieldRef, Formula.Apply)):
                token = "u-"

            if isinstance(token, (Formula.List, Formula.Constant, Formula.FieldRef)):
                token._depth = depth
                token._count = 0
                valstack.append(token)
                prevtoken = token
                token, remaining = cls._gettoken(remaining)

                if token not in [",", ")", "]", None] + cls._binary.keys():
                    raise defs.FormulaParsingError("expression %s cannot be followed by \"%s\"" % (prevtoken, token))

            else:
                if prevtoken == "," and token in cls._binary: raise defs.FormulaParsingError("binary operator \"%s\" cannot come right after a \",\"" % token)

                if token == "[":
                    value, remaining = cls._parse(remaining, level="[]", depth=depth+1)

                    expr = Formula.List()
                    for v in value:
                        expr.addArgument(v)
                    expr._depth = depth
                    expr._count = 0
                    valstack.append(expr)

                    prevtoken = token
                    token, remaining = cls._gettoken(remaining)

                elif token == "]":
                    if level != "[]": raise defs.FormulaParsingError("unopened square bracket")

                    while len(oprstack) > 1: valstack, oprstack = cls._reduce(oprstack[-1], valstack, oprstack, depth)
                    return valstack, remaining

                elif token == "(":
                    value, remaining = cls._parse(remaining, level="()", depth=depth+1)

                    if len(value) != 1: raise defs.FormulaParsingError("parenthetical phrase contains commas (and is not an argument list)")
                    valstack.append(value[0])

                    prevtoken = token
                    token, remaining = cls._gettoken(remaining)

                elif token == ")":
                    if level != "()": raise defs.FormulaParsingError("unopened parenthesis")

                    while len(oprstack) > 1: valstack, oprstack = cls._reduce(oprstack[-1], valstack, oprstack, depth)
                    return valstack, remaining

                elif token == ",":
                    if len(oprstack) == 1 and len(valstack) == 0:
                        raise defs.FormulaParsingError("expression may not begin with \",\"")

                    while len(oprstack) > 1: valstack, oprstack = cls._reduce(oprstack[-1], valstack, oprstack, depth)
                    prevtoken = token
                    token, remaining = cls._gettoken(remaining)

                elif isinstance(token, Formula.Apply):
                    prevtoken = token
                    token, remaining = cls._gettoken(remaining)
                    if token != "(": raise defs.FormulaParsingError("function name must be followed by \"(\"")  # can't get here?

                    value, remaining = cls._parse(remaining, level="()", depth=depth+1)
                    prevtoken.addArguments(value)
                    prevtoken._depth = depth
                    prevtoken._count = 0
                    valstack.append(prevtoken)

                    prevtoken = token
                    token, remaining = cls._gettoken(remaining)

                else:
                    top = oprstack[-1]
                    action = cls._action[top][token]

                    if action == cls._SHIFT:
                        oprstack.append(token)
                        prevtoken = token
                        token, remaining = cls._gettoken(remaining)

                    elif action == cls._REDUCE:
                        valstack, oprstack = cls._reduce(top, valstack, oprstack, depth)

                    elif action == cls._ACCEPT:
                        if level == "()": raise defs.FormulaParsingError("unclosed parenthesis")
                        if level == "[]": raise defs.FormulaParsingError("unclosed square bracket")
                        if len(valstack) == 0: raise defs.FormulaParsingError("empty expression")

                        return valstack

                    else:
                        raise defs.FormulaParsingError(action)

    @classmethod
    def parse(cls, text):
        """Parse a formula, producing an internal syntax tree.

        @type text: string
        @param text: The formula to parse.
        @rtype: Formula.List, Formula.Constant, Formula.FieldRef, or Formula.Apply
        @return: A syntax tree represented by nested class instances.
        """

        if text is None or text.strip() == "":
            raise defs.PmmlValidationError("Formula is empty")

        result = cls._parse(text)
        if len(result) != 1:
            raise defs.PmmlValidationError("Formula evaluates to %d expressions, rather than 1" % len(result))
        result = result[0]

        if isinstance(result, cls.List):
            raise defs.PmmlValidationError("Formula evaluates to a list, rather than Constant, FieldRef, or Apply")

        return result

    def evaluate(self, dataTable, functionTable, performanceTable, text=None):
        """Evaluate the expression, using a DataTable as input.

        @type dataTable: DataTable
        @param dataTable: The input DataTable, containing any fields that might be used to evaluate this expression.
        @type functionTable: FunctionTable
        @param functionTable: The FunctionTable, containing any functions that might be called in this expression.
        @type performanceTable: PerformanceTable
        @param performanceTable: A PerformanceTable for measuring the efficiency of the calculation.
        @type text: string or None
        @param text: If None, use the text of this Formula object; otherwise, use C{text} instead.
        @rtype: DataColumn
        @return: The result of the calculation as a DataColumn.
        """

        if text is None:
            text = self.text

        performanceTable.begin("Formula parse")
        parsed = Formula.parse(text)
        performanceTable.end("Formula parse")

        performanceTable.begin("Formula evaluate")
        dataColumn = parsed.evaluate(dataTable, functionTable, performanceTable)

        if dataColumn.mask is None:
            return dataColumn

        data = dataColumn.data
        mask = dataColumn.mask
        mask = FieldCastMethods.applyInvalidValueTreatment(mask, self.get("invalidValueTreatment"))
        data, mask = FieldCastMethods.applyMapMissingTo(dataColumn.fieldType, data, mask, self.get("mapMissingTo"))

        performanceTable.end("Formula evaluate")
        return DataColumn(dataColumn.fieldType, data, mask)

    @classmethod
    def expansion(cls, modelLoader, text):
        """Derive the expanded form of a text-based formula.

        @type modelLoader: ModelLoader
        @param modelLoader: A ModelLoader that is used to construct the new PMML elements.
        @type text: string
        @param text: The formula as a string.
        @rtype: PmmlBinding
        @return: The PMML elements.
        """

        E = modelLoader.elementMaker()
        return cls.parse(text).asPmml(E)

    def expand(self, modelLoader):
        """Replace this Formula with the equivalent PMML.

        @type modelLoader: ModelLoader
        @param modelLoader: A ModelLoader that is used to construct the new PMML elements.
        """

        expansion = Formula.expansion(modelLoader, self.text)
        parent = self.getparent()
        index = parent.index(self)
        del parent[index]
        parent.insert(index, expansion)
