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

"""This module defines the TreeModel for strict PMML."""

from augustus.pmml.model.trees.TreeModel import TreeModel
from augustus.pmml.model.trees.Node import Node

def register(modelLoader):
    """Add TreeModel classes to a ModelLoader's C{tagToClass} map.

    @type modelLoader: ModelLoader
    @param modelLoader: The ModelLoader to modify.
    """

    modelLoader.register("TreeModel", TreeModel)
    modelLoader.register("Node", Node)
