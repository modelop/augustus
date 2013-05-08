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

"""This module defines the PoissonDistribution class."""

import math

from augustus.core.defs import defs
from augustus.core.NumpyInterface import NP, gammaln, gammainc
from augustus.core.PmmlBinding import PmmlBinding

class PoissonDistribution(PmmlBinding):
    """Represents a Poisson distribution for BaselineModels."""

    def cdf(self, array):
        """Vectorized cumulative distribution function (CDF).

        @type array: 1d Numpy array of numbers
        @param array: The input vector.
        @rtype: 1d Numpy array of numbers
        @return: The result of CDF_Poisson(x) for all input values x.
        """

        try:
            import scipy.special
        except ImportError:
            pass
        else:
            gammainc = lambda a, x: NP(scipy.special.gammainc(a, x))

        return gammainc(float(self.attrib["mean"]), NP("floor", NP(array + 1.0)))

    def pdf(self, array):
        """Vectorized probability density function (PDF).

        @type array: 1d Numpy array of numbers
        @param array: The input vector.
        @rtype: 1d Numpy array of numbers
        @return: The result of PDF_Poisson(x) for all input values x.
        """

        return NP("exp", self.logpdf(array))

    def logpdf(self, array):
        """Vectorized logarithm of the probability density function (PDF).

        @type array: 1d Numpy array of numbers
        @param array: The input vector.
        @rtype: 1d Numpy array of numbers
        @return: The result of ln(PDF_Poisson(x)) for all input values x.
        """

        try:
            import scipy.special
        except ImportError:
            pass
        else:
            gammaln = lambda x: NP(scipy.special.gammaln(x))

        mean = float(self.attrib["mean"])
        return NP(NP(NP(array * math.log(mean)) - gammaln(NP(array + 1.0))) - mean)
