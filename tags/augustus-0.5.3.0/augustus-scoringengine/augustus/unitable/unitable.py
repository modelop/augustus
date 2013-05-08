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

import os
import re
import itertools
import codecs
import math

import numpy

from augustus.core.python3transition import *
from augustus.unitable.unipage import BeyondPageException, UniPage, UniPageOnDisk, UniPageDiskCacheManager
from augustus.unitable.utilities import IncompatibleFilesInChain, UninitializedError, BadlyFormattedInputData, INVALID, MISSING, cast, typeToDtype, recognizedDtypes, getfiles, getformat
from augustus.unitable.streaminputs import CSVStream, XMLStream
from augustus.unitable.evaluate import parse, FieldExpression

from augustus.core.xmlbase import load
import augustus.unitable.xtbl10 as xtbl

default = {"pageSize": 1024}

def readUniTable(fileLocation, format=None, sorter=None, pageSize=None, mapInvalid=None, mapMissing=None, **parameters):
    format = getformat(fileLocation, format)

    ################################################################ CSV
    if format == "CSV":
        csvInput = CSVStream(fileLocation, sorter, **parameters)

        if csvInput.types is not None:
            types = csvInput.types
        else:
            types = dict((f, "string") for f in csvInput.fields)

        _mapInvalid = dict((f, str("INVALID") if types[f] in ("category", "string") else -1000) for f in csvInput.fields)
        if mapInvalid is None:
            mapInvalid = _mapInvalid
        else:
            _mapInvalid.update(mapInvalid)
            mapInvalid = _mapInvalid

        _mapMissing = dict((f, str("MISSING") if types[f] in ("category", "string") else -1000) for f in csvInput.fields)
        if mapMissing is None:
            mapMissing = _mapMissing
        else:
            _mapMissing.update(mapMissing)
            mapMissing = _mapMissing

        table = UniTable(csvInput.fields, types)
        table.initMemory(pageSize)

        for record in csvInput:
            table.fill([mapInvalid[f] if r is INVALID else mapMissing[f] if r is MISSING else r for f, r in zip(csvInput.fields, record)])

        return table

    ################################################################ XML
    if format == "XML":
        xmlInput = XMLStream(fileLocation, sorter, **parameters)

        if xmlInput.types is not None:
            types = xmlInput.types
        else:
            types = dict((f, "string") for f in xmlInput.fields)

        _mapInvalid = dict((f, str("INVALID") if types[f] in ("category", "string") else -1000) for f in xmlInput.fields)
        if mapInvalid is None:
            mapInvalid = _mapInvalid
        else:
            _mapInvalid.update(mapInvalid)
            mapInvalid = _mapInvalid

        _mapMissing = dict((f, str("MISSING") if types[f] in ("category", "string") else -1000) for f in xmlInput.fields)
        if mapMissing is None:
            mapMissing = _mapMissing
        else:
            _mapMissing.update(mapMissing)
            mapMissing = _mapMissing

        table = UniTable(xmlInput.fields, types)
        table.initMemory(pageSize)

        for record in xmlInput:
            table.fill([mapInvalid[f] if r is INVALID else r for f, r in [(f, record.get(f, mapMissing[f])) for f in xmlInput.fields]])

        return table

    ################################################################ NAB
    elif format == "NAB":
        fileNames = getfiles(fileLocation, sorter)
        if len(fileNames) == 0:
            raise IOError("No files match \"%s\" (even with wildcards)" % fileLocation)

        fields = None
        types = None
        strings = {}
        args = {}
        for fileName in fileNames:
            file = open(fileName, "rb")
            header = file.readline().rstrip()
            file.close()

            headerfields = header.decode("utf-8").split()
            if headerfields[0] != "RecArray":
                raise BadlyFormattedInputData("NAB file \"%s\" does not begin with 'RecArray'" % fileName)

            args[fileName] = dict(asciistr(f).split("=") for f in headerfields[1:])

            if "masktype" in args.keys():
                raise NotImplementedError("No support yet for NAB files (such as \"%s\") with masked NumPy arrays" % fileName)

            if set(args[fileName].keys()) != set(["formats", "names"]):
                raise BadlyFormattedInputData("NAB file \"%s\" headers are %s, rather than set([\"formats\", \"names\"])" % (fileName, str(set(args[fileName].keys()))))

            thisfields = args[fileName]["names"].split(",")
            thistypes = args[fileName]["formats"].split(",")
            for i in xrange(len(thistypes)):
                if thistypes[i][0] == "a":
                    thistypes[i] = "string"
                    strings[thisfields[i]] = True
                else:
                    strings[thisfields[i]] = False

            if fields is None:
                fields = thisfields
                types = thistypes
            else:
                if fields != thisfields:
                    raise IncompatibleFilesInChain("NAB file \"%s\" header has fields %s, which differ from the first %s" % (fileName, str(thisfields), str(fields)))
                if types != thistypes:
                    raise IncompatibleFilesInChain("NAB file \"%s\" header has types %s, which differ from the first %s" % (fileName, str(thistypes), str(types)))

        table = UniTable(fields, dict(zip(fields, types)))
        table.pages = []
        table.starts = []
        table.length = 0

        for fileName in fileNames:
            file = open(fileName, "rb")
            file.readline()
            data = numpy.rec.fromfile(file, **args[fileName])
            
            table.pageSize = len(data)
            page = UniPage(table.fields, table.types)

            arrays = {}
            for f in table.fields:
                arr = data.field(f)
                if strings[f]:
                    arr = [i.decode("utf-8") for i in arr]
                arrays[f] = arr

            page.initExisting(table.pageSize, arrays, copy=False, stringToCategory=True)
            table.pages.append(page)
            table.starts.append(table.length)
            table.length += len(data)

        return table

    ################################################################ XTBL
    elif format == "XTBL":
        fileNames = getfiles(fileLocation, sorter)
        if len(fileNames) == 0:
            raise IOError("No files match \"%s\" (even with wildcards)" % fileLocation)

        limitGB = parameters.get("limitGB", None)
        memoryMap = parameters.get("memoryMap", False)

        # get the footers from each file (XML) and make sure they have identical DataDictionaries
        footers = []
        for i, fileName in enumerate(fileNames):
            fileSize = os.stat(fileName).st_size
            file = open(fileName, "rb")

            file.seek(max(0, fileSize - 1024))
            text = file.read()
            m = re.search("<SeekFooter\s+byteOffset=\"([0-9]+)\"\s+/>", text)
            if m is not None:
                textStart = int(m.group(1))
            else:
                raise IOError("File \"%s\" does not have the right format (the <SeekFooter /> element was not found in the last kilobyte)" % fileName)

            file.seek(textStart)

            footer = load(file.read(), xtbl.XTBL)
            footers.append(footer)
            if len(footers) > 1:
                thisDataDictionary = footer.child(xtbl.DataDictionary)
                firstDataDictionary = footers[0].child(xtbl.DataDictionary)

                if thisDataDictionary != firstDataDictionary:
                    for x in thisDataDictionary.matches(xtbl.LookupTable, maxdepth=None) + firstDataDictionary.matches(xtbl.LookupTable, maxdepth=None):
                        x.serialize()
                    raise IncompatibleFilesInChain("XTBL file \"%s\" is incompatible with the first file \"%s\":%s%s%s%s" % (fileNames[i], fileNames[0], os.linesep, thisDataDictionary.xml(), os.linesep, firstDataDictionary.xml()))

            file.close()

        # set up the UniTable's fields, types, pages, starts, and length
        fields = []
        types = {}
        dtypes = {}
        lookups = {}

        for dataField in footers[0].child(xtbl.DataDictionary).matches(xtbl.DataField):
            field = dataField.attrib["name"]
            fields.append(field)
            types[field] = dataField.attrib["type"]
            dtypes[field] = dataField.attrib["dtype"]

            lookup = dataField.child(xtbl.LookupTable, exception=False)
            if lookup is not None:
                lookups[field] = lookup.n_to_v
            else:
                lookups[field] = None

        categories = []
        for f in fields:
            n_to_v = lookups[f]
            if n_to_v is None:
                categories.append(None)
            else:
                v_to_n = dict((v, n) for n, v in n_to_v.items())
                categories.append((v_to_n, n_to_v))

        table = UniTable(fields, types)
        table.pages = []
        table.starts = []
        table.length = 0

        uniPageDiskCacheManager = UniPageDiskCacheManager(limitGB, memoryMap)

        for i, fileName in enumerate(fileNames):
            for xtblpage in footers[i].child(xtbl.Pages).matches(xtbl.Page):
                length = xtblpage.attrib["length"]

                byteOffsets = {}
                for pageFieldOffset in xtblpage.matches(xtbl.PageFieldOffset):
                    byteOffsets[pageFieldOffset.attrib["name"]] = pageFieldOffset.attrib["byteOffset"]

                uniPage = UniPageOnDisk(fields, table.types)
                uniPage.initDisk(length, fileName, byteOffsets, dtypes, categories, uniPageDiskCacheManager)

                table.pages.append(uniPage)
                table.starts.append(table.length)
                table.length += length

        return table

class UniTable(object):
    comma = str(",")

    def __init__(self, fields, types):
        for t in types.values():
            if t not in cast and t not in recognizedDtypes:
                raise ValueError("unrecognized UniTable type \"%s\"" % t)

        if len(fields) == 0:
            raise ValueError("UniTable must have at least one field")

        self.fields = fields
        self.types = [types[f] for f in fields]
        self._types = types

        self._file = None
        self._format = None
        self.pageLimit = None

    def initMemory(self, pageSize=None):
        if pageSize is None:
            pageSize = default["pageSize"]

        if pageSize <= 0:
            raise ValueError("UniTable pageSize must be positive (not %d)" % pageSize)

        self.pageSize = pageSize
        page = UniPage(self.fields, self.types)
        page.initMemory(self.pageSize)
        self.pages = [page]
        self.starts = [0]
        self.length = 0

    def initExisting(self, values, copy=True):
        if set(values.keys()) != set(self.fields):
            raise ValueError("fields in initExisting (%s) differ from original fields (%s)" % (str(set(values.keys())), str(set(self.fields))))

        lengths = [len(arr) for arr in values.values()]
        same = [l == lengths[0] for l in lengths]
        if False in same:
            raise ValueError("arrays have different lengths: %s" % lengths)

        self.pageSize = lengths[0]
        page = UniPage(self.fields, self.types)
        page.initExisting(self.pageSize, values, copy=copy)
        self.pages = [page]
        self.starts = [0]
        self.length = self.pageSize

    def fill(self, values):
        try:
            self.pages[-1].fill(values)
            self.length += 1

        except BeyondPageException:
            self.starts.append(self.starts[-1] + self.pages[-1].allocation)

            self._writing()

            page = UniPage(self.fields, self.types)
            page.initMemory(self.pageSize)
            page.categories = self.pages[-1].categories
            self.pages.append(page)
            self.pages[-1].fill(values)
            self.length += 1

            self._cullPages()

        except AttributeError:
            raise UninitializedError("UniTable initMemory or initExisting must be called before fill")

    def fillpage(self, values, copy=True):
        if set(values.keys()) != set(self.fields):
            raise ValueError("fields in initExisting (%s) differ from original fields (%s)" % (str(set(values.keys())), str(set(self.fields))))

        lengths = [len(arr) for arr in values.values()]
        same = [l == lengths[0] for l in lengths]
        if False in same:
            raise ValueError("arrays have different lengths: %s" % lengths)

        if not hasattr(self, "pages"):
            raise UninitializedError("UniTable initMemory or initExisting must be called before fillpage")

        page = UniPage(self.fields, self.types)
        page.initExisting(lengths[0], values, copy=copy)

        if self.pages[-1].length == 0:
            # empty page (probably just called initMemory); replace it and leave the starts list as it is
            self.pages[-1] = page
            
        else:
            # non-empty page (either full from fillpage() or partially full from fill()); write it an add this new page
            # _writing() has protection against being called twice
            self._writing()
            self.pages.append(page)
            self.starts.append(self.starts[-1] + lengths[0])

        # either way, update lengths
        self.length += lengths[0]

        # write out the new page and cull any excess (_cullPages() also has protection against being called twice)
        self._writing()
        self._cullPages()

    def _cullPages(self):
        if self.pageLimit is not None and len(self.pages) > self.pageLimit:
            lengthLost = sum([p.length for p in self.pages[:-self.pageLimit]])
            self.length -= lengthLost
            self.pages = self.pages[-self.pageLimit:]
            self.starts = [s - lengthLost for s in self.starts[-self.pageLimit:]]
        
    def parse(self, expression):
        return parse(expression, self.fields, self._types)

    def histogram(self, expr, cuts=None, weights=None, bins=10, range=None, categoryToString=True, callback=None):
        # TODO: if the result is a string, you should be doing 'unique' on it instead

        if isinstance(expr, basestring): expr = self.parse(expr)
        if isinstance(cuts, basestring): cuts = self.parse(cuts)
        if isinstance(weights, basestring): weights = self.parse(weights)

        if len(expr) != 1: raise ValueError("The expression contains more than one element (%d)" % len(expr))
        if cuts is not None and len(cuts) != 1: raise ValueError("The cuts expression contains more than one element (%d)" % len(cuts))
        if weights is not None and len(weights) != 1: raise ValueError("The weights expression contains more than one element (%d)" % len(weights))

        fields = set()
        expr[0].findFields(fields)
        if cuts is not None: cuts[0].findFields(fields)
        if weights is not None: weights[0].findFields(fields)

        if not hasattr(self, "pages"):
            raise UninitializedError("UniTable initMemory or initExisting must be called before histogram")

        lookups = {}
        for i in xrange(len(self.fields)):
            if self.types[i] == "category":
                lookups[self.fields[i]] = self.pages[-1].categories[i]
        
        hist = None
        bin_edges = None

        for theStart, page in zip(self.starts, self.pages):
            if cuts is not None:
                cutsResult = page.evaluate(cuts, fields, lookups, categoryToString=False)
            
            exprResult = page.evaluate(expr, fields, lookups, categoryToString=False)
            if cuts is not None:
                exprResult = numpy.extract(cutsResult, exprResult)

            if weights is not None:
                weightsResult = page.evaluate(weights, fields, lookups, categoryToString=True)
                if cuts is not None:
                    weightsResult = numpy.extract(cutsResult, weightsResult)
            else:
                weightsResult = None
                
            if hist is None:
                hist, bin_edges = numpy.histogram(exprResult, bins=bins, range=range, weights=weightsResult)
                if range is None:
                    range = bin_edges[0], bin_edges[-1]
            else:
                hist2, bin_edges2 = numpy.histogram(exprResult, bins=bins, range=range, weights=weightsResult)
                numpy.add(hist, hist2, hist)

            if callback is not None:
                callback(hist, bin_edges)

        return hist, bin_edges

    # def evaluate(self, expr, maxLength=None, sufficientNonzero=None, categoryToString=True):
    def evaluate(self, expr, maxLength=None, categoryToString=True):
        if isinstance(expr, basestring):
            expr = self.parse(expr)

        fields = set()
        for v in expr:
            v.findFields(fields)

        if not hasattr(self, "pages"):
            raise UninitializedError("UniTable initMemory or initExisting must be called before evaluate")

        lookups = {}
        for i in xrange(len(self.fields)):
            if self.types[i] == "category":
                lookups[self.fields[i]] = self.pages[-1].categories[i]

        if maxLength is None:
            length = self.length
        else:
            length = maxLength
        outputs = [numpy.empty(length, dtype=v.type) if v.type != "S" else [] for v in expr]

        validLength = 0
        # numNonzero = [0]*len(expr)

        for theStart, page in zip(self.starts, self.pages):
            result = page.evaluate(expr, fields, lookups, categoryToString=categoryToString)

            breaknow = False
            for i in xrange(len(expr)):
                if isinstance(outputs[i], list):
                    if len(outputs[i]) + len(result[i]) > length:
                        outputs[i].extend(result[i][:length - len(outputs[i])])
                        if i == 0: validLength += length - len(outputs[i])
                        breaknow = True
                    else:
                        outputs[i].extend(result[i])
                        if i == 0: validLength += len(result[i])

                else:
                    if theStart+page.length > length:
                        outputs[i][theStart:length] = result[i][:length - theStart]
                        if i == 0: validLength += length - theStart
                        breaknow = True
                    else:
                        outputs[i][theStart:theStart+page.length] = result[i]
                        if i == 0: validLength += len(result[i])

                        # if sufficientNonzero is not None:
                        #     numNonzero[i] += numpy.count_nonzero(result[i])

            if breaknow: break

            # if sufficientNonzero is not None:
            #     if all([nnz > sufficientNonzero for nnz in numNonzero]):
            #         break

        # if sufficientNonzero is not None and validLength < length:
        #     for i in xrange(len(expr)):
        #         outputs[i] = outputs[i][:validLength]

        return outputs

    def select(self, expr, cuts=None, maxLength=None, categoryToString=True):
        if cuts is None:
            return self.evaluate(expr, maxLength=maxLength, categoryToString=categoryToString)

        if isinstance(expr, basestring): expr = self.parse(expr)
        if isinstance(cuts, basestring): cuts = self.parse(cuts)

        exprFields, cutsFields = set(), set()
        for v in expr: v.findFields(exprFields)
        for v in cuts: v.findFields(cutsFields)

        if not hasattr(self, "pages"):
            raise UninitializedError("UniTable initMemory or initExisting must be called before select")

        lookups = {}
        for i in xrange(len(self.fields)):
            if self.types[i] == "category":
                lookups[self.fields[i]] = self.pages[-1].categories[i]

        outputs = [[] for i in xrange(len(expr))]
        validLength = 0

        for theStart, page in zip(self.starts, self.pages):
            indicators = page.evaluate(cuts, cutsFields, lookups, categoryToString=categoryToString)
            indicator = indicators[0]
            for another in indicators[1:]:
                numpy.logical_and(indicator, another, indicator)

            thisLength = len(numpy.nonzero(indicator)[0])
            validLength += thisLength

            if thisLength > 0:
                result = page.evaluate(expr, exprFields, lookups, categoryToString=categoryToString)

                for i, v in enumerate(expr):
                    if v.type == "S":
                        result[i] = [result[i][j] for j in indicator if indicator[j]]
                    else:
                        result[i] = numpy.extract(indicator, result[i])

                    outputs[i].append(result[i])

            if maxLength is not None and validLength >= maxLength:
                break

        for i, v in enumerate(expr):
            if v.type == "S":
                outputs[i] = sum(outputs[i], [])
            else:
                outputs[i] = numpy.concatenate(outputs[i])

            if maxLength is not None and validLength > maxLength:
                outputs[i] = outputs[i][:maxLength]

        return outputs

        # if cuts is None:
        #     return self.evaluate(expr, maxLength=maxLength, categoryToString=categoryToString)

        # indicators = self.evaluate(cuts, sufficientNonzero=maxLength, categoryToString=categoryToString)
        # indicator = indicators[0]
        # for another in indicators[1:]:
        #     numpy.logical_and(indicator, another, indicator)

        # outputs = self.evaluate(expr, maxLength=len(indicator), categoryToString=categoryToString)

        # for i in xrange(len(outputs)):
        #     if isinstance(outputs[i], list):
        #         outputs[i] = [outputs[i][j] for j in indicator if indicator[j]]
        #     else:
        #         outputs[i] = numpy.extract(indicator, outputs[i])

        #     if len(outputs[i]) > maxLength:
        #         outputs[i] = outputs[i][:maxLength]

        # return outputs

    def scan(self, expr=None, cuts=None, categoryToString=True, maxLength=10, columnWidth=12, sep=" ", end=None, file=None):
        if expr is None:
            expr = [FieldExpression(f, t) for f, t in zip(self.fields, self.types)]
        elif isinstance(expr, basestring):
            expr = self.parse(expr)

        arrays = self.select(expr, cuts, maxLength, categoryToString=categoryToString)
        titles = map(repr, expr)

        formatTitles = []
        separator = []
        formatLine = []
        typechar = []
        for title, array in zip(titles, arrays):
            formatTitles.append("%%%d.%ds" % (columnWidth, columnWidth))
            separator.append("=" * columnWidth)

            if isinstance(array, list):
                formatLine.append("%%%ds" % columnWidth)
                typechar.append(" ")
            elif array.dtype.char in numpy.typecodes["Float"]:
                formatLine.append("%%%dg" % columnWidth)
                typechar.append("f")
            elif array.dtype.char in numpy.typecodes["AllInteger"]:
                formatLine.append("%%%dd" % columnWidth)
                typechar.append("i")
            elif array.dtype.char == "?":
                formatLine.append("%%%ds" % columnWidth)
                typechar.append("?")
            elif array.dtype.char in numpy.typecodes["Complex"]:
                formatLine.append("%%%dg+%%%dgj" % ((columnWidth-2)//2, (columnWidth-2)//2))
                typechar.append("F")
            elif array.dtype.char in numpy.typecodes["Character"] + "Sa":
                formatLine.append("%%%d.%ds" % (columnWidth, columnWidth))
                typechar.append("S")

        formatTitles = sep.join(formatTitles)
        separator = sep.join(separator)

        print3(formatTitles % tuple(titles), end=end, file=file)
        print3(separator, end=end, file=file)

        for i in xrange(len(arrays[0])):
            line = []
            for array, f, c in zip(arrays, formatLine, typechar):
                if c == " ":
                    line.append(f % array[i])
                elif c == "F":
                    line.append(f % (array[i].real, array[i].imag))
                elif c == "?":
                    if array[i]:
                        line.append(f % "True")
                    else:
                        line.append(f % "False")
                elif c == "S":
                    line.append(f % ("'%s'" % array[i]))
                else:
                    line.append(f % array[i])
            print3(sep.join(line), end=end, file=file)

    def getitem(self, field, index, categoryToString=True):
        if field not in self.fields:
            raise KeyError("unrecognized UniTable field \"%s\"" % field)

        if index < 0:
            index = self.length + index

        if not (0 <= index < self.length):
            raise IndexError("UniTable index (%d) out of range [0-%d)" % (index, self.length))

        try:
            i = next(i-1 for i, start in enumerate(self.starts) if start > index)
        except StopIteration:
            i = -1

        return self.pages[i].getitem(field, index - self.starts[i], categoryToString=categoryToString)

    def getcolumn(self, field, categoryToString=True):
        if field not in self.fields:
            raise KeyError("unrecognized UniTable field \"%s\"" % field)

        t = self._types[field]
        if t == "string" or (categoryToString and t == "category"):
            output = list(itertools.chain(*[p.getcolumn(field, categoryToString=categoryToString) for p in self.pages]))
        else:
            output = numpy.empty(self.length, dtype=typeToDtype.get(t, t))
            for theStart, page in zip(self.starts, self.pages):
                output[theStart:theStart+page.length] = page.getcolumn(field, categoryToString=categoryToString)

        return output

    def getrow(self, index, categoryToString=True):
        if index < 0:
            index = self.length + index

        if not (0 <= index < self.length):
            raise IndexError("UniTable index (%d) out of range [0-%d)" % (index, self.length))

        try:
            i = next(i-1 for i, start in enumerate(self.starts) if start > index)
        except StopIteration:
            i = -1

        return self.pages[i].getrow(index - self.starts[i], categoryToString=categoryToString)

    def iter(self, categoryToString=True):
        for page in self.pages:
            for index in xrange(page.length):
                yield page.getrow(index, categoryToString=categoryToString)

    def __iter__(self):
        return self.iter()

    def __len__(self):
        return self.length
    
    def initialize(self):
        self._pageIndex = 0
        self._indexOnPage = -1
        self._available = False
        
    def next(self):
        try:
            self._indexOnPage += 1
        except AttributeError:
            return UninitializedError("UniTable initialize() must be called before next()")

        try:
            if self._indexOnPage >= self.pages[self._pageIndex].length:
                self._pageIndex += 1
                self._indexOnPage = 0
        except IndexError:
            raise StopIteration

        self._available = True

    def flush(self):
        self._available = False        

    def get(self, field):
        if not self._available: return MISSING
        try:
            page = self.pages[self._pageIndex]
        except AttributeError:
            raise UninitializedError("UniTable next() must be called before get()")
        except IndexError:
            raise StopIteration

        return page.getitem(field, self._indexOnPage)

    def stringLength(self, field):
        try:
            i = self.fields.index(field)
        except ValueError:
            raise ValueError("unrecognized field name \"%s\"" % field)

        if self.types[i] not in ("category", "string"):
            raise ValueError("field name \"%s\" has type \"%s\"; it is not a string" % (field, self.types[i]))

        if not hasattr(self, "pages"):
            raise UninitializedError("UniTable initMemory or initExisting must be called before longestString")

        if len(self.pages) == 0: return 0

        if self.types[i] == "category":
            v_to_n = self.pages[-1].categories[i][0]
            themax = 0
            for v in v_to_n.keys():
                length = len(v.encode("utf-8"))
                if length > themax:
                    themax = length
            return themax

        else:
            themax = 0
            for page in self.pages:
                if page.length == 0:
                    continue
                elif page.length == page.allocation:
                    page.load([field])
                    ends = page.data[i]
                else:
                    page.load([field])
                    ends = page.data[i][:page.length]
                starts = numpy.roll(ends, 1)
                starts[0] = 0
                longest = (ends - starts).max()
                if longest > themax:
                    themax = longest
            return themax

    def _write_CSV_header(self, fileName):
        file = codecs.open(fileName, "w", encoding="utf-8")
        file.write(self.comma.join(["%s [%s]" % (f, self._types[f]) for f in self.fields]))
        file.write(os.linesep)
        return file

    def _write_NAB_header(self, fileName):
        types = list(self.types)
        types2 = list(self.types)
        for i in xrange(len(types)):
            if types[i] in ("category", "string"):
                stringLength = self.stringLength(self.fields[i])
                if stringLength == 0: stringLength = 256
                types[i] = "a%d" % stringLength
                types2[i] = "|S%d" % stringLength
            else:
                types[i] = typeToDtype.get(types[i], types[i])
                types2[i] = typeToDtype.get(types2[i], types[i])
        file = open(fileName, "wb")
        file.write("RecArray names=".encode("utf-8"))
        file.write(self.comma.join(self.fields).encode("utf-8"))
        file.write(" formats=".encode("utf-8"))
        file.write(self.comma.join(types).encode("utf-8"))
        file.write("\n".encode("utf-8"))
        return file, types2

    def _write_XML_header(self, fileName):
        file = codecs.open(fileName, "w", encoding="utf-8")
        file.write("<table tag=\"row\" fields=\"%s\" types=\"%s\">" % (",".join(self.fields), ",".join(self.types)))
        file.write(os.linesep)
        return file

    def _write_XML_footer(self, file):
        file.write("</table>")
        file.write(os.linesep)

    def _prepare_XTBL_footer(self, fileName):
        file = open(fileName, "wb")
        footer = xtbl.root(self.fields, self._types)
        footer.pages = footer.child(xtbl.Pages)
        footer.fields = {}
        footer.lookups = {}
        for f in self.fields:
            footer.fields[f] = footer.child(xtbl.DataDictionary).child(lambda x: isinstance(x, xtbl.DataField) and x.attrib["name"] == f)
            if self._types[f] == "category":
                footer.lookups[f] = xtbl.LookupTable()
                footer.fields[f].children.append(footer.lookups[f])

        return file, footer

    def write(self, fileName, format=None):
        if format == "CSV":
            file = self._write_CSV_header(fileName)

            for page in self.pages:
                page.write_CSV(file)

            file.close()

        elif format == "XML":
            file = self._write_XML_header(fileName)

            for page in self.pages:
                page.write_XML(file)

            self._write_XML_footer(file)
            file.close()

        elif format == "NAB":
            file, types = self._write_NAB_header(fileName)

            for page in self.pages:
                page.write_NAB(file, types)

            file.close()

        elif format == "XTBL":
            file, footer = self._prepare_XTBL_footer(fileName)

            for page in self.pages:
                page.write_XTBL(file, footer)

            for lookup in footer.lookups.values():
                lookup.serialize()

            footer.child(xtbl.SeekFooter).attrib["byteOffset"] = file.tell()
            file.write(footer.xml().encode("utf-8"))
            file.write(os.linesep.encode("utf-8"))
            file.close()

        else:
            raise NotImplementedError("Unknown file format \"%s\"" % format)

    def writing(self, fileName, format="CSV"):
        if format == "CSV":
            self._file = self._write_CSV_header(fileName)

        elif format == "XML":
            self._file = self._write_XML_header(fileName)

        elif format == "NAB":
            self._file, self._types = self._write_NAB_header(fileName)

        elif format == "XTBL":
            self._file, self._footer = self._prepare_XTBL_footer(fileName)
            
        else:
            raise NotImplementedError("Unknown file format \"%s\"" % format)

        self._format = format
        self.pageLimit = 1

    def _writing(self):
        if hasattr(self, "_file") and self._file is not None and not hasattr(self.pages[-1], "_written"):
            if self._format == "CSV":
                self.pages[-1].write_CSV(self._file)

            elif self._format == "XML":
                self.pages[-1].write_XML(self._file)

            elif self._format == "NAB":
                self.pages[-1].write_NAB(self._file, self._types)

            elif self._format == "XTBL":
                self.pages[-1].write_XTBL(self._file, self._footer)

            else:
                pass

            self.pages[-1]._written = True
            
    def close(self):
        self._writing()

        if hasattr(self, "_file") and self._file is not None:
            if self._format == "XML":
                self._write_XML_footer(self._file)

            elif self._format == "XTBL":
                for lookup in self._footer.lookups.values():
                    lookup.serialize()

                self._footer.child(xtbl.SeekFooter).attrib["byteOffset"] = self._file.tell()

                self._file.write(self._footer.xml().encode(errors="replace"))
                self._file.write(os.linesep)

            self._file.close()
            self._file = None

    def __del__(self):
        self.close()
