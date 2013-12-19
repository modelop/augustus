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

"""This module defines the GaussianDistribution class."""

import math

from augustus.core.defs import defs
from augustus.core.NumpyInterface import NP, erf
from augustus.core.PmmlBinding import PmmlBinding

class GaussianDistribution(PmmlBinding):
    """Represents a Gaussian distribution for BaselineModels."""

    def cdf(self, array):
        """Vectorized cumulative distribution function (CDF).

        @type array: 1d Numpy array of numbers
        @param array: The input vector.
        @rtype: 1d Numpy array of numbers
        @return: The result of CDF_Gaussian(x) for all input values x.
        """

        try:
            import scipy.special
        except ImportError:
            pass
        else:
            erf = lambda x: NP(scipy.special.erf(x))

        mean = float(self.attrib["mean"])
        root2sigma = math.sqrt(2.0*float(self.attrib["variance"]))
        return NP(NP(erf(NP(NP(array - mean)/root2sigma)) + 1.0)/2.0)

    def pdf(self, array):
        """Vectorized probability density function (PDF).

        @type array: 1d Numpy array of numbers
        @param array: The input vector.
        @rtype: 1d Numpy array of numbers
        @return: The result of PDF_Gaussian(x) for all input values x.
        """

        mean = float(self.attrib["mean"])
        twovariance = 2.0 * float(self.attrib["variance"])
        return NP(NP("exp", NP(NP("negative", NP("square", NP(array - mean))) / twovariance))/math.sqrt(math.pi*twovariance))

    def logpdf(self, array):
        """Vectorized logarithm of the probability density function (PDF).

        @type array: 1d Numpy array of numbers
        @param array: The input vector.
        @rtype: 1d Numpy array of numbers
        @return: The result of ln(PDF_Gaussian(x)) for all input values x.
        """

        mean = float(self.attrib["mean"])
        twovariance = 2.0 * float(self.attrib["variance"])
        return NP(NP(NP("negative", NP("square", NP(array - mean))) / twovariance) - math.log(math.sqrt(math.pi*twovariance)))
