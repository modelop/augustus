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

"""This module defines the ModelToolkit class."""

from augustus.core.Toolkit import Toolkit
from augustus.core.PmmlBinding import PmmlBinding
from augustus.core.PmmlModel import PmmlModel

class ModelToolkit(Toolkit):
    """A Toolkit interface for building PMML models, intended for
    interactive analysis and building workflows via templates.

    All functions in this toolkit share a ModelLoader for convenience.
    """

    importables = ["wrap"]

    def __init__(self, modelLoader, copyright=None):
        """Initialize the ModelToolkit with a ModelLoader and optional copyright message.

        This ModelLoader will be used to build all PMML elements
        on demand.

        @type modelLoader: ModelLoader
        @param modelLoader: The ModelLoader.
        @type copyright: string or None
        @param copyright: The copyright message to insert into the <Header> of all auto-constructed models.
        """

        super(ModelToolkit, self).__init__(modelLoader=modelLoader, copyright=copyright)

    def _checkOptions(self, options, expectedOptions):
        unrecognizedOptions = set(options) - set(expectedOptions)
        if len(unrecognizedOptions) > 0:
            raise TypeError("Unrecognized options: %s" % " ".join(unrecognizedOptions))

        for name, value in expectedOptions.items():
            if name not in options:
                options[name] = value

    def wrap(self, fieldTypes, *contents, **options):
        """Create a PMML model from transformations, models, or plots.

        @type fieldTypes: dict
        @param fieldTypes: Map from field name (string) to data type (string).
        @param *contents: PMML transformations, models, or plots.
        @type categoricalStrings: bool
        @param categoricalStrings: If True, all string DataFields will be categorical; if False, all string DataFields will be continuous (keyword only).
        @raise PmmlValidationError: If the resulting configuration is not valid PMML, this function raises an error.
        @rtype: PmmlBinding
        @return: A PMML document containing the C{contents}.
        """

        self._checkOptions(options, {"categoricalStrings": True})

        E = self.modelLoader.elementMaker()

        output = E.PMML(version=self.modelLoader.tagToClass["PMML"].version)

        if self.copyright is None:
            output.append(E.Header())
        else:
            output.append(E.Header(copyright=self.copyright))

        dataDictionary = E.DataDictionary()
        for fieldName, dataType in fieldTypes.items():
            optype = "continuous"
            if options["categoricalStrings"] and dataType == "string":
                optype = "categorical"

            dataDictionary.append(E.DataField(name=fieldName, dataType=dataType, optype=optype))

        output.append(dataDictionary)

        if len(contents) == 1 and isinstance(contents[0], PmmlModel):
            output.append(contents[0])
        else:
            output.append(E.TransformationDictionary(*contents))

        self.modelLoader.validate(output)
        return output
