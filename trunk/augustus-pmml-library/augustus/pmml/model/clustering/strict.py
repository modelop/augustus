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

"""This module defines the ClusteringModel for strict PMML."""

from augustus.pmml.model.clustering.ClusteringModel import ClusteringModel
from augustus.pmml.model.clustering.ComparisonMeasure import ComparisonMeasure
from augustus.pmml.model.clustering.ClusteringField import ClusteringField

from augustus.pmml.model.clustering.Euclidean import Euclidean
from augustus.pmml.model.clustering.SquaredEuclidean import SquaredEuclidean
from augustus.pmml.model.clustering.Chebychev import Chebychev
from augustus.pmml.model.clustering.CityBlock import CityBlock
from augustus.pmml.model.clustering.Minkowski import Minkowski
from augustus.pmml.model.clustering.SimpleMatching import SimpleMatching
from augustus.pmml.model.clustering.Jaccard import Jaccard
from augustus.pmml.model.clustering.Tanimoto import Tanimoto
from augustus.pmml.model.clustering.BinarySimilarity import BinarySimilarity

def register(modelLoader):
    """Add ClusteringModel classes to a ModelLoader's C{tagToClass} map.

    @type modelLoader: ModelLoader
    @param modelLoader: The ModelLoader to modify.
    """

    modelLoader.register("ClusteringModel", ClusteringModel)
    modelLoader.register("ComparisonMeasure", ComparisonMeasure)
    modelLoader.register("ClusteringField", ClusteringField)

    modelLoader.register("euclidean", Euclidean)
    modelLoader.register("squaredEuclidean", SquaredEuclidean)
    modelLoader.register("chebychev", Chebychev)
    modelLoader.register("cityBlock", CityBlock)
    modelLoader.register("minkowski", Minkowski)
    modelLoader.register("simpleMatching", SimpleMatching)
    modelLoader.register("jaccard", Jaccard)
    modelLoader.register("tanimoto", Tanimoto)
    modelLoader.register("binarySimilarity", BinarySimilarity)
