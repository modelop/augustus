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

"""This module defines the SquaredEuclidean class."""

import math

from augustus.core.NumpyInterface import NP
from augustus.pmml.model.clustering.PmmlClusteringMetric import PmmlClusteringMetric

class SquaredEuclidean(PmmlClusteringMetric):
    """The SquaredEuclidean metric is the same thing as the Euclidean, but without taking a square root at the end."""

    def initialize(self, state, numberOfRecords, numberOfFields, distributionBased):
        """First step in a vectorized metric calculation with missing values, called once before all fields and cluster centers.

        Only modifies the C{state} object.

        @type state: ad-hoc Python object
        @param state: State information that persists long enough to span the three steps of a metric calculation.  This is a work-around of lxml's refusal to let its Python instances maintain C{self} and it is unrelated to DataTableState.
        @type numberOfRecords: int
        @param numberOfRecords: The number of rows in the dataset.
        @type numberOfFields: int
        @param numberOfFields: The number of columns in the dataset.
        @type distributionBased: bool
        @param distributionBased: If True, use a covariance matrix to scale the distance result.
        """

        state.sumInQuadrature = NP("zeros", numberOfRecords, dtype=NP.dtype(float))
        if distributionBased:
            state.displacements = NP("empty", (numberOfRecords, numberOfFields), dtype=NP.dtype(float))
            state.displacementIndex = 0

    def accumulate(self, state, cxy, fieldWeight, distributionBased):
        """Second step in a vectorized metric calculation, called for each field and cluster center.

        Only modifies the C{state} object.

        @type state: ad-hoc Python object
        @param state: State information that persists long enough to span the three steps of a metric calculation.  This is a work-around of lxml's refusal to let its Python instances maintain C{self} and it is unrelated to DataTableState.
        @type cxy: 1d Numpy array of numbers
        @param cxy: Comparison distance or similarity for all rows.
        @type fieldWeight: number
        @param fieldWeight: The weight of this field.
        @type distributionBased: bool
        @param distributionBased: If True, use a covariance matrix to scale the distance result.
        """

        NP("add", state.sumInQuadrature, NP(NP(cxy**2) * fieldWeight), state.sumInQuadrature)

        if distributionBased:
            state.displacements[:,state.displacementIndex] = NP(cxy * math.sqrt(fieldWeight))
            state.displacementIndex += 1

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

        if adjustM is None:
            result = state.sumInQuadrature
        else:
            result = NP(state.sumInQuadrature * adjustM)

        if distributionBased:
            normalizations = NP("sqrt", NP("sum", NP(state.displacements**2), axis=1))
            selection = NP(normalizations > 0.0)
            state.displacements[selection] = state.displacements[selection] / (normalizations[:, NP.newaxis])[selection]

            lengthOfSigma = NP("sum", NP(NP(state.displacements.dot(covarianceMatrix)) * state.displacements), axis=1)

            result[selection] = NP(result[selection] / lengthOfSigma[selection])

        return result
