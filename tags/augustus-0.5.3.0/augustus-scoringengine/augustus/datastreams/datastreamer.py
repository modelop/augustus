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

"""Defines a class that abstracts the process of reading in data."""

import sys

from augustus.core.defs import Atom, INVALID, MISSING
import augustus.core.xmlbase as xmlbase
import augustus.core.pmml41 as pmml
import augustus.core.config as config

from augustus.unitable.utilities import INVALID as uINVALID, MISSING as uMISSING
from augustus.unitable.unitable import readUniTable
from augustus.unitable.streaminputs import CSVStream, CSVStreamFromStream, XMLStream, XMLStreamFromStream, NABStream

class DataIngestError(IOError): pass

class DataStreamer:
    def __init__(self, fromFile, model):
        parameters = fromFile.parameters
        checkHeaders = True

        if isinstance(fromFile, config.FromFile):
            if fromFile["format"] == "XTBL":
                allParameters = {"limitGB": 0, "memoryMap": False}
                allParameters.update(parameters)  # user-specified parameters outweigh the defaults
                allParameters["format"] = "XTBL"  # except for this one
                self.inputStream = readUniTable(fromFile["fileLocation"], **allParameters)

            elif fromFile["format"] == "CSV":
                allParameters = dict(parameters)
                allParameters["types"] = None  # casting should be done by DataContext, not CSVStream
                self.inputStream = CSVStream(fromFile["fileLocation"], **allParameters)

            elif fromFile["format"] == "XML":
                allParameters = dict(parameters)
                allParameters["types"] = None  # casting should be done by DataContext, not CSVStream
                self.inputStream = XMLStream(fromFile["fileLocation"], **allParameters)
                checkHeaders = False

            elif fromFile["format"] == "NAB":
                allParameters = dict(parameters)
                self.inputStream = NABStream(fromFile["fileLocation"], **allParameters)

            else: assert False

        elif isinstance(fromFile, config.FromStandardIn):
            if fromFile["format"] == "CSV":
                allParameters = {"sep": ","}
                allParameters.update(parameters)

                if "header" not in allParameters:
                    raise config.ConfigurationError("FromStandardIn is missing required parameter 'header' (add a <Parameter name='header' value='...' /> block inside <FromStandardIn>)")

                if "skipHeader" in allParameters:
                    try:
                        allParameters["skipHeader"] = xmlbase.validateBoolean(allParameters["skipHeader"])
                    except xmlbase.XMLValidationError as err:
                        raise config.ConfigurationError("skipHeader must be boolean: %s" % str(err))

                self.inputStream = CSVStreamFromStream(sys.stdin, None, **allParameters)

            elif fromFile["format"] == "XML":
                allParameters = dict(parameters)
                self.inputStream = XMLStreamFromStream(sys.stdin, None, **allParameters)
                checkHeaders = False

            else: assert False
        else: assert False

        if checkHeaders:
            inModelNotInData = set(model.dataContext.dataDictionary.dataFields.keys()).difference(set(self.inputStream.fields))
            if len(inModelNotInData) > 0:
                raise DataIngestError("The following fields are requested by the model's MiningSchema but are not in the input file(s): %s" % ", ".join(inModelNotInData))
        self.initialize = self.inputStream.initialize
        self.next = self.inputStream.next
        self.flush = self.inputStream.flush

    def get(self, field):
        output = self.inputStream.get(field)
        if output is uINVALID: return INVALID
        if output is uMISSING: return MISSING
        return output
