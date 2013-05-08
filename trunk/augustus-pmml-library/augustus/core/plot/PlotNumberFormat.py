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

"""This module defines the PlotNumberFormat class."""

import math
import re

class PlotNumberFormat(object):
    """PlotNumberFormat is a bag of functions that are useful for
       representing numbers in plots.
       """

    _re_firstDigits = re.compile("([0-9\.]+)", re.UNICODE)

    @staticmethod
    def toUnicode(x, low=None, high=None, tryPrecision=False):
        """Convert a number to a Unicode string with appropriately
           formatted minus signs and exponents.

           The parameters C{low}, C{high} (must both be non-None to work)
           and C{tryPrecision} (an independent criterion) are intended
           for better number formatting in some special cases of
           plotting.  (The plot focuses on a narrow range and we need the
           numbers on the tick marks to be distinct.)

           @type x: number
           @param x: The number to convert to a Unicode string.
           @type low: number or None
           @param low: Attempt to override standard "%g" formatting with knowledge of the low and high boundaries of a plot.
           @type high: number or None
           @param high: Attempt to override standard "%g" formatting with knowledge of the low and high boundaries of a plot.
           @type tryPrecision: bool
           @param tryPrecision: If True, try for high precision even if C{low} and C{high} don't indicate a good range.
           @rtype: unicode string
           @return: A formatted string representing the number C{x}.
           """

        if isinstance(x, basestring):
            output = unicode(x)
        elif low is not None and high is not None and (tryPrecision or (low*high > 0.0 and 2.0*(high - low)/abs(high + low) < 0.1)):
            precision = 1 - int(round(math.log10(high - low)))
            if precision > 0:
                formatter = u"%%.%df" % precision
                output = formatter % x
            else:
                output = u"%g" % x
        else:
            output = u"%g" % x

        if output[0] == u"-":
            output = u"\u2212" + output[1:]

        index = output.find(u"e")
        if index != -1:
            uniout = unicode(output[:index]) + u"\u00d710"
            sawNonzero = False
            for n in output[index+1:]:
                if n == u"+": pass # uniout += u"\u207a"
                elif n == u"-":
                    uniout += u"\u207b"
                elif n == u"0":
                    if sawNonzero:
                        uniout += u"\u2070"
                elif n == u"1":
                    sawNonzero = True
                    uniout += u"\u00b9"
                elif n == u"2":
                    sawNonzero = True
                    uniout += u"\u00b2"
                elif n == u"3":
                    sawNonzero = True
                    uniout += u"\u00b3"
                elif u"4" <= n <= u"9":
                    sawNonzero = True
                    if sawNonzero: uniout += eval("u\"\\u%x\"" % (0x2070 + ord(n) - ord("0")))
                else: uniout += n

            if uniout[:2] == u"1\u00d7": uniout = uniout[2:]
            return uniout

        return output

    @staticmethod
    def roundDigits(x, N=2):
        """Round a number to a specified number of digits, then format it with C{toUnicode}.

           @type x: number
           @param x: The number to round and convert to a Unicode string.
           @type N: integer
           @param N: The number of digits.
           @rtype: unicode string
           @return: A formatted string representing the number C{x} with C{N} digits of precision.
           """

        output = ("%%.%dg" % N) % x
        firstDigits = re.search(PlotNumberFormat._re_firstDigits, output)
        if firstDigits is not None and len(firstDigits.group(1)) < N:
            index = firstDigits.span()[1]
            if firstDigits.group(1).find(".") == -1:
                output = "".join([output[:index], "."] + (["0"] * (N - len(firstDigits.group(1)))) + [output[index:]])
            else:
                output = "".join([output[:index]] + (["0"] * (N - len(firstDigits.group(1)))) + [output[index:]])
        return PlotNumberFormat.toUnicode(output)
