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

"""This module defines the Matrix class."""

import re

from augustus.core.defs import defs
from augustus.core.PmmlBinding import PmmlBinding
from augustus.core.PmmlArray import PmmlArray

class Matrix(PmmlBinding):
    """Matrix implements an explicit, symmetric, or sparse matrix of constants.

    U{PMML specification<http://www.dmg.org/v4-1/GeneralStructure.html>}.
    """

    def values(self, convertType=False):
        """Interpret the matrix and extract values.

        @type convertType: bool
        @param convertType: If False, return values as strings; if True, convert them to floating point numbers.
        @rtype: list of lists
        @return: The explicit matrix as nested Python lists.
        @raise PmmlValidationError: If incorrect PMML is met, this function will raise an error.
        """

        kind = self.get("kind", defaultFromXsd=True)

        if kind == "diagonal":
            if len(self.childrenOfTag("MatCell")) != 0:
                raise defs.PmmlValidationError("A diagonal matrix must be defined entirely with Arrays, not MatCells")

            array = self.childOfClass(PmmlArray)
            if array is None:
                raise defs.PmmlValidationError("A diagonal matrix must contain an array of on-diagonal values")
            array = array.values(convertType=convertType)

            nbRows = self.get("nbRows", len(array), convertType=True)
            nbCols = self.get("nbCols", len(array), convertType=True)
            if nbRows != len(array) or nbCols != len(array):
                raise defs.PmmlValidationError("Diagonal matrix declared with nbRows=%s nbCols=%s, yet it contains an array of length %d" % (self.get("nbRows", "(unset)"), self.get("nbCols", "(unset)"), len(array)))

            offDiagDefault = self.get("offDiagDefault", "0.0")
            if convertType:
                offDiagDefault = float(offDiagDefault)

            return [[array[i] if i == j else offDiagDefault for j in xrange(nbCols)] for i in xrange(nbRows)]

        elif kind == "symmetric":
            if len(self.childrenOfTag("MatCell")) != 0:
                raise defs.PmmlValidationError("A diagonal matrix must be defined entirely with Arrays, not MatCells")

            arrays = self.childrenOfClass(PmmlArray)
            for row in xrange(len(arrays)):
                arrays[row] = arrays[row].values(convertType=convertType)
                if row != len(arrays[row]):
                    raise defs.PmmlValidationError("Symmetric matrix declared with array of the wrong length: row %d has length %d" % (row, len(arrays[row])))

            rows = row + 1

            nbRows = self.get("nbRows", rows, convertType=True)
            nbCols = self.get("nbCols", rows, convertType=True)
            if nbRows != rows or nbCols != rows:
                raise defs.PmmlValidationError("Symmetric matrix declared with nbRows=%s nbCols=%s, yet the longest array has length %d" % (self.get("nbRows", "(unset)"), self.get("nbCols", "(unset)"), rows))

            return [[array[i][j] if j < i else array[j][i] for j in xrange(nbCols)] for i in xrange(nbRows)]

        elif kind == "any":
            arrays = self.childrenOfClass(PmmlArray)
            matcells = self.childrenOfTag("MatCell")

            if len(arrays) > 0 and len(matcells) == 0:
                cols = None
                for row in xrange(len(arrays)):
                    arrays[row] = arrays[row].values(convertType=convertType)
                    if cols is None:
                        cols = len(arrays[row])
                    elif cols != len(arrays[row]):
                        raise defs.PmmlValidationError("Explicit matrix must consist of equal-length Arrays")

                rows = row + 1

                nbRows = self.get("nbRows", rows, convertType=True)
                nbCols = self.get("nbCols", cols, convertType=True)
                if nbRows != rows or nbCols != cols:
                    raise defs.PmmlValidationError("Explicit matrix declared with nbRows=%s nbCols=%s, yet it contains %d Arrays of length %d" % (self.get("nbRows", "(unset)"), self.get("nbCols", "(unset)"), row, col))

                return [[arrays[i][j] for j in xrange(nbCols)] for i in xrange(nbRows)]
            
            elif len(arrays) == 0:
                diagDefault = self.get("diagDefault")
                offDiagDefault = self.get("offDiagDefault")
                if diagDefault is None or offDiagDefault is None:
                    raise defs.PmmlValidationError("Sparse matrix must have \"diagDefault\" and \"offDiagDefault\" explicitly set")

                if convertType:
                    diagDefault = float(diagDefault)
                    offDiagDefault = float(offDiagDefault)
                
                lookup = {}
                maxrow = None
                maxcol = None
                for matcell in matcells:
                    value = matcell.text
                    if value is None:
                        value = ""
                    else:
                        value = value.strip()

                    row = int(matcell["row"])
                    col = int(matcell["col"])
                    lookup[row - 1, col - 1] = value

                    if maxrow is None or row > maxrow: maxrow = row
                    if maxcol is None or col > maxcol: maxcol = col

                nbRows = self.get("nbRows", maxrow, convertType=True)
                nbCols = self.get("nbCols", maxcol, convertType=True)

                return [[lookup[i,j] if (i,j) in lookup else (diagDefault if i == j else offDiagDefault) for j in xrange(nbCols)] for i in xrange(nbRows)]

            else:
                raise defs.PmmlValidationError("A matrix must be defined by Arrays or MatCells, but not both")
