#!/usr/bin/env python

# Copyright (C) 2006-2011  Open Data ("Open Data" refers to
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

"""Defines a scheme for creating new segments on the fly."""

import logging

import augustus.core.config as config
import augustus.core.pmml41 as pmml
from augustus.core.defs import Atom
from augustus.core.config import ConfigurationError
from augustus.core.xmlbase import XMLComment

class SegmentationScheme(object):
    """
    """
    def __init__(self, configOptions, pmmlModel):
        """Set up segmentation according to user preference.

        Arguments:

            configOptions: an XML element (from xmlbase)
            of type config.SegmentationScheme
            (tags <SegmentationScheme> ... </SegmentationScheme>)
        """
        self._pmml = pmmlModel
        self._dataTypes = self._pmml.topModels[0].dataContext.cast
        self._logger = logging.getLogger()
        self._metadata = logging.getLogger("metadata")
        self._blackList = None
        self._generic = None
        self._whiteList = None
        self._template = None
        self._segmentList = None

        if configOptions is None:
            return
        else:
            self._generic = configOptions.generic
            self._whiteList = configOptions.whiteList
            self._blackList = configOptions.blackList
            if self._whiteList is not None or self._generic is not None:
                self._setSegmentTemplate()

        for field, typeconvert in self._dataTypes.iteritems():
            if self._blackList:
                for definition in self._blackList:
                    if field in definition:
                        self._convertFieldTypes(
                            definition, field, typeconvert)
            if self._whiteList:
                for definition in self._whiteList:
                    if field in definition:
                        self._convertFieldTypes(
                            definition, field, typeconvert)
            if self._generic:
                for definition in self._generic:
                    if field in definition:
                        self._convertFieldTypes(
                            definition, field, typeconvert)

    def blacklisted(self, get):
        if self._blackList:
            return self._getMatchWithin(self._blackList, get, checkOnly=True)
        else:
            return False

    def _compoundAnd(self, *predicates):
        if len(predicates) == 0:
            raise Exception("Encountered a list of zero predicates in SegmentationScheme's _compoundAnd; this should not ever be possible.")
        elif len(predicates) == 1:
            return predicates[0]
        else:
            return pmml.newInstance("CompoundPredicate", attrib={"booleanOperator": "and"}, children=predicates)

    def _compoundRange(self, rangeTuple):
        f, low, high, closure = rangeTuple
        opL = "greaterOrEqual" if closure.startswith('c') else "greaterThan"
        opH = "lessOrEqual" if closure.endswith('d') else "lessThan"
        if high is None:
            return self._simplePredicate(field=f, operator=opL, value=low)
        elif low is None:
            return self._simplePredicate(field=f, operator=opH, value=high)
        else:
            p1 = self._simplePredicate(field=f, operator=opL, value=low)
            p2 = self._simplePredicate(field=f, operator=opH, value=high)
            return pmml.newInstance("CompoundPredicate", attrib={"booleanOperator": "and"}, children=[p1, p2])

    def _convertFieldTypes(self, definition, field, typeconvert):
        """Convert field's types in definition using typeconvert.

        Meant only to be called from self.initialize().

        Arguments:

            definition (NameSpace object)
                contains four items:
                include_items, exclude_items, ranges, partitions

            field (string)
                The field name.

            typeconvert (function)
                A type conversion, maybe with additional checks.
        """
        def cast(x):
            try:
                return typeconvert(x)
            except:
                return x

        try:
            if definition[field].include_items is not None:
                definition[field].exclude_items = [cast(x) for x in definition[field].exclude_items]
                definition[field].include_items = set([cast(x) for x in definition[field].include_items])
        except ValueError:
            raise ConfigurationError("Could not convert enumerated item with field=%s in configuration file segment definition to the appropriate data type as defined in the pmml." % field)
        try:
            definition[field].ranges = [
                (cast(l), cast(r), closure, divisions, index)
                for l, r, closure, divisions, index
                in definition[field].ranges]
            definition[field].partitions = [(cast(l), cast(r), closure) for l, r, closure in definition[field].partitions]
        except ValueError:
            raise ConfigurationError("Could not convert partitioned item with field=%s in configuration file segment definition to the appropriate data type as defined in the pmml." % field)

    def _createPMMLSegment(self, matchList):
        predicateList = [self._tupleToPredicate(match) for match in matchList]

        if len(predicateList) == 0:
            raise Exception("Encountered a list of zero predicates in SegmentationScheme's _createPMMLSegment; this should not ever be possible when using a validated config file.")
        elif len(predicateList) == 1:
            newPredicate = predicateList[0]
        else:
            newPredicate = self._compoundAnd(*predicateList)

        newSegment = self._template.copy()
        del newSegment['id']
        def replaceFalse(x):
            return newPredicate if isinstance(x, pmml.pmmlFalse) else x
        newSegment.children = [replaceFalse(x) for x in newSegment.children]

        # Validate and set up the new model instance
        model = newSegment.child(pmml.isModel)
        for element in reversed(newSegment.matches(lambda x: hasattr(x, "post_validate"), maxdepth=None)):
            element.post_validate()

        miningSchema = model.child(pmml.MiningSchema)
        parentContext = self._template.model.dataContext.parent
        miningSchema.top_validate_parentContext(parentContext, parentContext.dataDictionary)
        localTransformations = model.child(
            pmml.LocalTransformations, exception=False)

        for element in model.matches(pmml.Apply, maxdepth=None):
            element.top_validate_transformationDictionary(self._pmml.transformationDictionary)

        model.dataContext = pmml.DataContext(
            parentContext,
            parentContext.dataDictionary,
            parentContext.transformationDictionary,
            miningSchema,
            localTransformations,
            model["functionName"])
        model.dataContext.clear()  # set up the overrides

        for element in reversed(newSegment.matches(lambda x: hasattr(x, "top_validate"), maxdepth=None)):
            element.top_validate(model.dataContext)

        # The below finds pointers that may be to dummy aggregates
        # and reassigns them to the new Segment's aggregates
        # No checking for groupField collisions is necessary, because
        # the SegmentTemplate was checked on PMML read.
        allDerivedFields = newSegment.matches(
            pmml.DerivedField, maxdepth=None)
        dataFieldMap = dict([
            (f["name"], f) for f in allDerivedFields if "name" in f.attrib])
        for derivedField in allDerivedFields:
            aggregates = derivedField.matches(pmml.Aggregate, maxdepth=None)
            if len(aggregates) != 0:
                derivedField.aggregates = aggregates
            for child in derivedField.matches(pmml.FieldRef, maxdepth=None):
                if child["field"] in dataFieldMap:
                    # then need to reassign the child's pointer to
                    # the newly created objects.
                    field = dataFieldMap[child["field"]]
                    derivedField.aggregates.extend(field.aggregates)
            for child in derivedField.matches(
                pmml.X_ODG_AggregateReduce, maxdepth=None):

                if child["field"] in dataFieldMap:
                    field = dataFieldMap[child["field"]]
                    child.aggregates = field.aggregates

        self._segmentList.append(newSegment)
        return newSegment

    def getMatch(self, get):
        if self._whiteList:
            newSegment = self._getMatchWithin(self._whiteList, get)
            if newSegment is not None:
                return newSegment
        if self._generic:
            newSegment = self._getMatchWithin(self._generic, get)
            if newSegment is not None:
                return newSegment
        return None

    def _getMatchWithin(self, elementList, get, checkOnly=False):
        for definitions in elementList:
            matchList = []
            for fieldName in definitions.keys():
                defn = definitions[fieldName]
                value = get(fieldName)
                if isinstance(value, Atom):
                    break

                matchTuple = None
                if defn.include_items == None:
                    matchTuple = fieldName, value, True
                elif value in defn.include_items:
                        matchTuple = fieldName, value, True
                elif len(defn.exclude_items) > 0 and value not in defn.exclude_items:
                    for elem in defn.exclude_items:
                        if value != elem:
                            matchTuple = fieldName, elem, False
                            break
                else:
                    for low, high, closure, divisions, i in defn.ranges:
                        inside = low < value and value < high
                        atLeft = closure.startswith('c') and low == value
                        atRight = closure.endswith('d') and high == value
                        if inside or atLeft or atRight:
                            for l, h, c in defn.partitions[i:]:
                                inside = l < value and value < h
                                atLeft = c.startswith('c') and l == value
                                atRight = c.endswith('d') and h == value
                                if inside or atLeft or atRight:
                                    if isinstance(h, Atom):
                                        h = None
                                    matchTuple = fieldName, l, h, c

                if matchTuple is not None:
                    matchList.append(matchTuple)
                else:
                    break
            if len(matchList) == len(definitions):
                break

        if len(matchList) != len(definitions):
            if checkOnly: return False
            else: return None
        else:
            if checkOnly:
                return True
            else:
                return self._createPMMLSegment(matchList)

    def _setSegmentTemplate(self):
        miningModel = self._pmml.child("MiningModel", exception=False)
        if miningModel is None:
            raise ConfigurationError("The top level PMML model must be \"MiningModel\" if segments are to be generated.")

        segmentation = miningModel.child("Segmentation", exception=False)
        self._segmentList = segmentation.children

        def isTemplate(x):
            if isinstance(x, pmml.Segment):
                if x.attrib.get("id", None) == "ODG-SegmentTemplate":
                    return True
            return False

        self._template = segmentation.child(isTemplate, exception=False)
        if self._template:
            self._template.model = self._template.child(pmml.isModel)
        else:
            raise ConfigurationError("Must have a segment template defined in the PMML file in order to produce segment...the segment must be in the MiningModel and must have the id=\"ODG-SegmentTemplate\" as its id and have <False /> as the selection predicate.")

        if not self._template.exists(pmml.pmmlFalse):
            raise ConfigurationError("The selection predicate must be <False /> for the default segment defined in the PMML model.")


    def _simplePredicate(self, field, value, operator):
        p = pmml.newInstance("SimplePredicate", attrib={"field": field, "value": value, "operator": operator})
        p.post_validate()
        return p

    def _tupleToPredicate(self, inputTuple):
        if len(inputTuple) == 3:
            operator = "equal" if inputTuple[2] is True else "notEqual"
            return self._simplePredicate(
                field=inputTuple[0], value=inputTuple[1], operator=operator)
        else:
            return self._compoundRange(inputTuple)
