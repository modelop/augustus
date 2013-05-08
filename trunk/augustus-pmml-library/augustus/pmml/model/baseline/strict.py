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

"""This module defines the BaselineModel for strict PMML."""

from augustus.pmml.model.baseline.BaselineModel import BaselineModel
from augustus.pmml.model.baseline.TestDistributions import TestDistributions
from augustus.pmml.model.baseline.GaussianDistribution import GaussianDistribution
from augustus.pmml.model.baseline.PoissonDistribution import PoissonDistribution

def register(modelLoader):
    """Add BaselineModel classes to a ModelLoader's C{tagToClass} map.

    @type modelLoader: ModelLoader
    @param modelLoader: The ModelLoader to modify.
    """

    modelLoader.register("BaselineModel", BaselineModel)
    modelLoader.register("TestDistributions", TestDistributions)
    modelLoader.register("GaussianDistribution", GaussianDistribution)
    modelLoader.register("PoissonDistribution", PoissonDistribution)
