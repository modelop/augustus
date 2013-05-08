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

"""This module defines the Chebychev class."""

from augustus.core.NumpyInterface import NP
from augustus.pmml.model.clustering.PmmlClusteringMetric import PmmlClusteringMetric

class Chebychev(PmmlClusteringMetric):
    """The Chebychev metric returns the maximum of its components, an L-infinity norm."""

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

        state.maximumComponent = NP("zeros", numberOfRecords, dtype=NP.dtype(float))
        if distributionBased:
            raise NotImplementedError("Distribution-based clustering has not been implemented for the %s metric" % self.t)

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

        NP("maximum", state.maximumComponent, NP(cxy * fieldWeight), state.maximumComponent)

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
            result = state.maximumComponent
        else:
            result = NP(state.maximumComponent * adjustM)
        return result
