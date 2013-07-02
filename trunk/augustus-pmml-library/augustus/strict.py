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

"""This module defines the strict interpretation of PMML.  It is
usually loaded as C{from augustus.strict import *}."""

from augustus.core.NumpyInterface import NP
NP("seterr", divide="ignore", invalid="ignore")  # handled by PMML (by returning INVALID instead of numbers)

### basic functionality
from augustus.core.defs import defs
from augustus.core.ModelLoader import ModelLoader
from augustus.core.PmmlBinding import PmmlBinding
from augustus.core.PmmlCalculable import PmmlCalculable
from augustus.core.PmmlModel import PmmlModel

### data input/output
from augustus.core.DataColumn import DataColumn
from augustus.core.DataTable import DataTable
from augustus.core.DataTableFields import DataTableFields
from augustus.core.DataTableState import DataTableState
from augustus.core.DataTablePlots import DataTablePlots
from augustus.core.FakeFieldType import FakeFieldType

### functions
from augustus.core.Function import Function
from augustus.core.FunctionTable import FunctionTable
from augustus.core.FunctionTableExtra import FunctionTableExtra

### profiling
from augustus.core.PerformanceTable import PerformanceTable

### PMML implementation
modelLoader = ModelLoader()

from augustus.pmml.PMML import PMML
modelLoader.register("PMML", PMML)
from augustus.pmml.DerivedField import DerivedField
modelLoader.register("DerivedField", DerivedField)
from augustus.pmml.MiningField import MiningField
modelLoader.register("MiningField", MiningField)
from augustus.pmml.OutputField import OutputField
modelLoader.register("OutputField", OutputField)
from augustus.pmml.DefineFunction import DefineFunction
modelLoader.register("DefineFunction", DefineFunction)

from augustus.pmml.ModelVerification import ModelVerification
modelLoader.register("ModelVerification", ModelVerification)
from augustus.pmml.VerificationField import VerificationField
modelLoader.register("VerificationField", VerificationField)

from augustus.pmml.Array import Array
modelLoader.register("Array", Array)
from augustus.pmml.INTSparseArray import INTSparseArray
modelLoader.register("INT-SparseArray", INTSparseArray)
from augustus.pmml.REALSparseArray import REALSparseArray
modelLoader.register("REAL-SparseArray", REALSparseArray)
from augustus.pmml.Matrix import Matrix
modelLoader.register("Matrix", Matrix)
from augustus.pmml.InlineTable import InlineTable
modelLoader.register("InlineTable", InlineTable)
from augustus.pmml.TableLocator import TableLocator
modelLoader.register("TableLocator", TableLocator)

from augustus.pmml.expression.Constant import Constant
modelLoader.register("Constant", Constant)
from augustus.pmml.expression.FieldRef import FieldRef
modelLoader.register("FieldRef", FieldRef)
from augustus.pmml.expression.NormContinuous import NormContinuous
modelLoader.register("NormContinuous", NormContinuous)
from augustus.pmml.expression.NormDiscrete import NormDiscrete
modelLoader.register("NormDiscrete", NormDiscrete)
from augustus.pmml.expression.Discretize import Discretize
modelLoader.register("Discretize", Discretize)
from augustus.pmml.expression.MapValues import MapValues
modelLoader.register("MapValues", MapValues)
from augustus.pmml.expression.Aggregate import Aggregate
modelLoader.register("Aggregate", Aggregate)
from augustus.pmml.expression.Apply import Apply
modelLoader.register("Apply", Apply)

from augustus.pmml.predicate.SimplePredicate import SimplePredicate
modelLoader.register("SimplePredicate", SimplePredicate)
from augustus.pmml.predicate.SimpleSetPredicate import SimpleSetPredicate
modelLoader.register("SimpleSetPredicate", SimpleSetPredicate)
from augustus.pmml.predicate.CompoundPredicate import CompoundPredicate
modelLoader.register("CompoundPredicate", CompoundPredicate)
from augustus.pmml.predicate.TRUE import TRUE
modelLoader.register("True", TRUE)
from augustus.pmml.predicate.FALSE import FALSE
modelLoader.register("False", FALSE)

### models
from augustus.pmml.model.segmentation.strict import *
register(modelLoader)
from augustus.pmml.model.baseline.strict import *
register(modelLoader)
from augustus.pmml.model.trees.strict import *
register(modelLoader)
from augustus.pmml.model.clustering.strict import *
register(modelLoader)

del register
