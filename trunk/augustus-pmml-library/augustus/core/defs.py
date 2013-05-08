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

"""This module defines global constants.  For consistency with all
other modules in Augustus, this module provides one object with the
same name as the module: C{defs}.

@type PmmlValidationError: subclass of StandardError
@param PmmlValidationError: Exception raised by badly-formed PMML elements.
@type FormulaParsingError: subclass of StandardError
@param FormulaParsingError: Exception raised by badly-formed formulae.

@type PMML_NAMESPACE: string
@param PMML_NAMESPACE: The PMML namespace.
@type XSD_NAMESPACE: string
@param XSD_NAMESPACE: The XSD namespace.
@type SVG_NAMESPACE: string
@param SVG_NAMESPACE: The SVG namespace.
@type XML_NAMESPACE: string
@param XML_NAMESPACE: The XML namespace.
@type XLINK_NAMESPACE: string
@param XLINK_NAMESPACE: The XLink namespace; used by some SVG elements.

@type XLINK_HREF: string
@param XLINK_HREF: "xlink::href"; used by some SVG elements.
@type XML_SPACE: string
@param XML_SPACE: "xml::space"; used by some SVG elements.

@type SVG_FILE_HEADER: string
@param SVG_FILE_HEADER: The first two lines of an SVG document (which is defined by DTD, not XSD).

@type PICKLE_XML_COMPRESSION: int
@param PICKLE_XML_COMPRESSION: The Gzip compression level for serializing PMML as compressed XML in Pickles.

@type NAN: float
@param NAN: The IEEE-754 "not a number" floating point value.

@type maskType: Numpy dtype
@param maskType: The dtype used by all mask arrays.

@type VALID: defs.maskType
@param VALID: The mask value corresponding to valid data (must be zero; many parts of the code make this assumption)
@type MISSING: defs.maskType
@param MISSING: The mask value corresponding to missing data (1).
@type INVALID: defs.maskType
@param INVALID: The mask value corresponding to invalid data (2).

@type PADDING: int
@param PADDING: A value (-1000) that is sometimes used in arrays when the mask is not C{VALID}.  One must never assume that a value of -1000 means invalid or that invalid implies that the data array will contain -1000.

@type EPSILON: float
@param EPSILON: A value that is "too small to see" on a plot (0.005).
@type INFINITY: float
@param INFINITY: A value that is "too large to see" on a plot (200).
"""

import new

from augustus.core.NumpyInterface import NP

class PmmlValidationError(StandardError): pass
class FormulaParsingError(StandardError): pass

PMML_NAMESPACE = "http://www.dmg.org/PMML-4_1"
XSD_NAMESPACE = "http://www.w3.org/2001/XMLSchema"
SVG_NAMESPACE = "http://www.w3.org/2000/svg"
XML_NAMESPACE = "http://www.w3.org/XML/1998/namespace"
XLINK_NAMESPACE = "http://www.w3.org/1999/xlink"

XLINK_HREF = "{%s}href" % XLINK_NAMESPACE
XML_SPACE = "{%s}space" % XML_NAMESPACE

SVG_FILE_HEADER = """<?xml version="1.0" standalone="no"?>
<!DOCTYPE svg PUBLIC "-//W3C//DTD SVG 1.1//EN" "http://www.w3.org/Graphics/SVG/1.1/DTD/svg11.dtd">
"""

PICKLE_XML_COMPRESSION = 1   # minimal Gzip compression when pickling PMML and ModelLoader schemae

NAN = float("NaN")     # used in enough places that there should be only one reference

maskType = NP.int8
VALID = maskType(0)    # VALID must be zero; many parts of the code make this assumption
MISSING = maskType(1)  # MISSING and INVALID (and any others) are exclusive states; values, not bits
INVALID = maskType(2)

PADDING = -1000        # optional "filler" for dataColumn.data when the mask says it's not VALID

EPSILON = 0.005        # a small difference to ignore while plotting
INFINITY = 200         # a ratio large enough to be effectively infinite while plotting

defs = new.module("augustus.core.defs")
defs.__dict__.update(globals())
