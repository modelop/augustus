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

import glob

from augustus.core.python3transition import *

class UninitializedError(IOError): pass
class IncompatibleFilesInChain(IOError): pass
class WrongFieldType(IOError): pass
class RequestingFieldNotInHeader(IOError): pass
class BadlyFormattedInputData(IOError): pass
class CannotCastObject(IOError): pass

INVALID = str("INVALID")
MISSING = str("MISSING")

def cast_impossible(x):
    raise CannotCastObject("Cannot cast text input data as a Python object, only primitive types")
def cast_string(x):
    if hasattr(x, "decode"):
        return x.decode("utf-8")
    else:
        return x
def cast_integer(x):
    try:
        return int(x)
    except ValueError:
        return INVALID
def cast_int64(x):
    try:
        return long(x)
    except ValueError:
        return INVALID
def cast_float(x):
    try:
        return float(x)
    except ValueError:
        return INVALID

cast = {"object": cast_impossible, "category": cast_string, "string": cast_string, "integer": cast_integer, "int64": cast_int64, "float": cast_float, "double": cast_float}
typeToDtype = {"object": None, "category": "<u4", "string": "<u8", "integer": "<i4", "int64": "<i8", "float": "<f", "double": "<f8"}
recognizedDtypes = set([byteorder + letter + number for byteorder in [">", "<", ""] for letter in ["i", "u", "f"] for number in ["4", "8", "16", ""]] + ["int8", "int16", "int32", "int64", "float16", "float32", "float64", "float128"])

def getfiles(fileLocation, sorter=None):
    # TODO: add "http://"
    fileNames = glob.glob(fileLocation)
    fileNames.sort(sorter)
    return fileNames

def getformat(fileLocation, format=None):
    if format is None:
        if fileLocation[-5:].upper() == ".XTBL":
            format = "XTBL"
        elif fileLocation[-4:].upper() == ".CSV":
            format = "CSV"
        elif fileLocation[-4:].upper() == ".XML":
            format = "XML"
        elif fileLocation[-4:].upper() == ".NAB":
            format = "NAB"
        else:
            raise ValueError("Unrecognized file format in fileLocation \"%s\"" % fileLocation)

    else:
        format = format.upper()
        if format not in ("XTBL", "CSV", "XML", "NAB"):
            raise NotImplementedError("Unknown file format \"%s\"" % format)

    return format
