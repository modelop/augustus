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

"""This module defines the BinarySimilarity class."""

from augustus.core.NumpyInterface import NP
from augustus.pmml.model.clustering.PmmlClusteringMetricBinary import PmmlClusteringMetricBinary

class BinarySimilarity(PmmlClusteringMetricBinary):
    """BinarySimilarity is a metric that generalizes the comparison of binary values."""

    def finalizeDistance(self, state, adjustM, distributionBased, covarianceMatrix):
        """Third and final step in a vectorized metric calculation, called once after all fields and cluster centers.

        Only modifes the C{state} object.

        @type state: ad-hoc Python object
        @param state: State information that persists long enough to span the three steps of a metric calculation.  This is a work-around of lxml's refusal to let its Python instances maintain C{self} and it is unrelated to DataTableState.
        @type adjustM: 1d Numpy array of numbers
        @param adjustM: The "adjustM" value, intended to adjust for missing values, as defined in the PMML specification.
        @type distributionBased: bool
        @param distributionBased: If True, use a covariance matrix to scale the distance result.
        @type covarianceMatrix: Numpy matrix
        @param covarianceMatrix: The covariance matrix to scale the result if C{distributionBased}.
        @rtype: 1d Numpy array of numbers
        @return: The array of distances or similarities for center-based clustering, and number of standard deviations for distribution-based clustering.
        """

        c00 = self["c00-parameter"]
        c01 = self["c01-parameter"]
        c10 = self["c10-parameter"]
        c11 = self["c11-parameter"]
        d00 = self["d00-parameter"]
        d01 = self["d01-parameter"]
        d10 = self["d10-parameter"]
        d11 = self["d11-parameter"]

        return NP(NP(NP(NP(NP(c11 * state.a11) + NP(c10 * state.a10)) + NP(c01 * state.a01)) + NP(c00 * state.a00)) /
                  NP(NP(NP(NP(d11 * state.a11) + NP(d10 * state.a10)) + NP(d01 * state.a01)) + NP(d00 * state.a00)))
