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

import sys
import re
import csv
import xml.parsers.expat

import numpy

from augustus.core.python3transition import *
from augustus.unitable.utilities import INVALID, MISSING, getfiles, cast, IncompatibleFilesInChain, WrongFieldType, RequestingFieldNotInHeader, BadlyFormattedInputData, UninitializedError

class CSVStream(object):
    def __init__(self, fileLocation, sorter=None, types="(.*)\s+\[(.*)\]", sniffLength=16*1024, header=None, sep=None):
        self.fileNames = getfiles(fileLocation, sorter)
        if len(self.fileNames) == 0:
            raise IOError("No files match \"%s\" (even with wildcards)" % fileLocation)

        self.dialect = None
        self.fields = None
        self.types = None

        self.explicitHeader = header
        if sep is not None and sys.version_info < (3,):
            sep = asciistr(sep)
        
        for fileName in self.fileNames:
            tmpfile = open(fileName, "r")
            thisDialect = csv.Sniffer().sniff(tmpfile.read(sniffLength))
            tmpfile.seek(0)

            if sep is not None:
                thisDialect.delimiter = sep

            reader = iter(csv.reader(tmpfile, thisDialect))
            thisFields = next(reader)

            if header is not None:
                numberOfRecords = len(thisFields)
                thisFields = header.split(thisDialect.delimiter)
                if len(thisFields) != numberOfRecords:
                    raise IncompatibleFilesInChain("explicitly set %d headers, but %s has %d columns in the first record" % (len(thisFields), fileName, numberOfRecords))

            if types == "sniff":
                thisTypes = {}
                for f, r in zip(thisFields, next(reader)):
                    try:
                        float(r)
                    except ValueError:
                        thisTypes[f] = "string"
                    else:
                        thisTypes[f] = "double"
                
            tmpfile.close()

            if self.dialect is None:
                self.dialect = thisDialect
                self.fields = thisFields
                if types == "sniff":
                    self.types = thisTypes

            else:
                if thisDialect.__dict__ != self.dialect.__dict__:
                    raise IncompatibleFilesInChain("the files %s cannot be chained because they have incompatible CSV dialects" % str(self.fileNames))
                if thisFields != self.fields:
                    raise IncompatibleFilesInChain("the files %s cannot be chained because they have incompatible CSV headers" % str(self.fileNames))
                if types == "sniff":
                    if thisTypes != self.types:
                        raise IncompatibleFilesInChain("the files %s cannot be chained because they have incompatible CSV field types" % str(self.fileNames))

        if types == "sniff":
            pass

        elif isinstance(types, basestring):
            self.types = {}
            for i in xrange(len(self.fields)):
                try:
                    f, t = re.match(types, self.fields[i]).groups()
                except (ValueError, AttributeError):
                    if types == "(.*)\s+\[(.*)\]":
                        raise WrongFieldType("header string \"%s\" did not match the default regular expression \"%s\":\n    if you want to use a different regular expression, pass types=\"regular expression...\" as an optional argument (needs two groups)\n    if you do not intend to auto-recognize field types (assume all are strings), pass types=None\n    if you want to try to guess the types from their values, pass types=\"sniff\"\n    if you want to supply explicit types, pass types={field: type, ...} for all fields" % (self.fields[i], types))

                    raise WrongFieldType("header string \"%s\" did not match and produce two groups from the regular expression \"%s\"" % (self.fields[i], types))

                self.fields[i] = f
                self.types[f] = t

                if t not in cast:
                    raise WrongFieldType("header string \"%s\" has unrecognized type \"%s\"" % (self.fields[i], t))

        elif isinstance(types, dict):
            for f, t in types.items():
                if f not in self.fields:
                    raise RequestingFieldNotInHeader("The requested field \"%s\" is not in the header of the CSV file" % f)
                if t not in cast:
                    raise WrongFieldType("field type \"%s\" is unrecognized" % t)

            self.types = types

        elif types is None:
            self.types = None

        else:
            raise TypeError("parameter 'types' must be \"sniff\", a field-type regular expression with two groups, a dictionary of explicit types, or None")

        if isinstance(self.types, dict) and all(map(callable, self.types.values())):
            self._types = self.types
        else:
            self._types = dict([(f, cast[t]) for f, t in (self.types.items() if self.types is not None else [(f, "string") for f in self.fields])])

    def __iter__(self):
        for fileName in self.fileNames:
            try:
                reader = csv.reader(open(fileName, "r"), self.dialect)
                if self.explicitHeader is None:
                    next(reader) # get past the header

                for record in reader:
                    if self.types is None:
                        yield record
                    else:
                        yield [self._types[f](r) for f, r in zip(self.fields, record)]

            except csv.Error as err:
                raise BadlyFormattedInputData("CSV reader encountered an error: %s" % str(err))

    def initialize(self):
        self._iterator = iter(self)

    def next(self):
        try:
            self._got = dict([(f, r) for f, r in zip(self.fields, next(self._iterator))])
        except AttributeError:
            raise UninitializedError("CSVStream initialize() must be called before next()")

    def flush(self):
        self._got = {}

    def get(self, field):
        try:
            return self._got.get(field, MISSING)
        except AttributeError:
            raise UninitializedError("CSVStream next() must be called before get()")

class CSVStreamFromStream(CSVStream):
    def __init__(self, stream, types, header, sep=",", skipHeader=False):
        self.stream = stream
        self.types = types
        self.skipHeader = skipHeader

        self.explicitHeader = header
        if sep is not None and sys.version_info < (3,):
            sep = asciistr(sep)

        self.dialect = csv.excel()
        if sep is not None: self.dialect.delimiter = sep

        self.fields = header.split(sep)

        if isinstance(self.types, dict) and all(map(callable, self.types.values())):
            self._types = self.types
        else:
            self._types = dict([(f, cast[t]) for f, t in (self.types.items() if self.types is not None else [(f, "string") for f in self.fields])])

    def __iter__(self):
        try:
            reader = csv.reader(self.stream, self.dialect)
            if self.skipHeader:
                next(reader) # get past the header

            for record in reader:
                if self.types is None:
                    yield record
                else:
                    yield [self._types[f](r) for f, r in zip(self.fields, record)]

        except csv.Error as err:
            raise BadlyFormattedInputData("CSV reader encountered an error: %s" % str(err))
        
class XMLStream(object):
    def __init__(self, fileLocation, sorter=None, types="sniff", blocksize=4096, fields=None):
        self.fileNames = getfiles(fileLocation, sorter)
        if len(self.fileNames) == 0:
            raise IOError("No files match \"%s\" (even with wildcards)" % fileLocation)

        self.blocksize = blocksize

        self.fields = None
        self.types = None

        for fileName in self.fileNames:
            self._checkfile = fileName
            thisFields, thisTypes = next(iter(self))
            self._checkfile = None

            if fields is not None:
                thisFields = fields

            if types == "sniff":
                for f, t in thisTypes.items():
                    if t not in cast:
                        raise WrongFieldType("header string \"%s\" has unrecognized type \"%s\"" % (f, t))
            else:
                thisTypes = types

            if self.fields is None:
                self.fields = thisFields
                self.types = thisTypes

            else:
                if thisFields != self.fields:
                    raise IncompatibleFilesInChain("the files %s cannot be chained because they have incompatible fields (determined from first record)" % str(self.fileNames))
                if thisTypes != self.types:
                    raise IncompatibleFilesInChain("the files %s cannot be chained because they have incompatible types (determined from 'type' attributes in the first record)" % str(self.fileNames))

        if types == "sniff":
            pass

        elif isinstance(types, dict):
            for f, t in types.items():
                if f not in self.fields:
                    raise RequestingFieldNotInHeader("the requested field \"%s\" is not in the first record of the first file" % f)
                if t not in cast:
                    raise WrongFieldType("field type \"%s\" is unrecognized" % t)
            self.types = types

        elif types is None:
            self.types = None

        else:
            raise TypeError("parameter 'types' must be \"sniff\", a dictionary of explicit types, or None")

        if isinstance(self.types, dict) and all(map(callable, self.types.values())):
            self._types = self.types
        else:
            self._types = dict([(f, cast[t]) for f, t in (self.types.items() if self.types is not None else [(f, "string") for f in self.fields])])

    def setupParser(self):
        def start_element(tag, attrib):
            if self._depth == 0:
                if "tag" in attrib:
                    self._rowtag = attrib["tag"]
                if "structure" in attrib:
                    if attrib["structure"] == "flatten":
                        self._rowflatten = True
                    elif attrib["structure"] == "ignore":
                        self._rowflatten = False
                    else:
                        raise BadlyFormattedInputData("'structure' attribute must either be \"flatten\" or \"ignore\".")
                if "fields" in attrib:
                    self._checkfields = [x.lstrip().rstrip() for x in attrib["fields"].split(",")]
                    if "types" in attrib:
                        self._checktypes = dict(zip(self._checkfields, [x.lstrip().rstrip() for x in attrib["types"].split(",")]))

            elif self._depth == 1 and self._rowtag is None:
                self._rowdepth = 1
                self._thisrow = {}

            elif tag == self._rowtag:
                self._rowdepth = 1
                self._thisrow = {}

            elif self._rowdepth >= 1:
                self._rowdepth += 1
                if self._rowflatten or self._rowdepth == 2:
                    if self._checktypes is not None:
                        if tag not in self._checktypes:
                            if "type" in attrib:
                                self._checktypes[tag] = attrib["type"]
                            else:
                                self._checktypes[tag] = "string"
                    self._thistext = []

            self._depth += 1

        def char_data(text):
            if (self._rowflatten and self._rowdepth >= 1) or self._rowdepth == 2:
                self._thistext.append(text)

        def end_element(tag):
            if self._rowdepth >= 2:
                if self._rowflatten or self._rowdepth == 2:
                    self._thisrow[tag] = "".join(self._thistext)
                self._rowdepth -= 1

            elif self._rowdepth == 1:
                self._rows.append(self._thisrow)
                self._rowdepth -= 1

            self._depth -= 1

        parser = xml.parsers.expat.ParserCreate()
        parser.StartElementHandler = start_element
        parser.CharacterDataHandler = char_data
        parser.EndElementHandler = end_element

        return parser

    def __iter__(self):
        parser = self.setupParser()

        if self._checkfile is not None: fileNames = [self._checkfile]
        else: fileNames = self.fileNames

        for fileName in fileNames:
            self._depth = 0
            self._rowtag = None
            self._rowflatten = None
            self._rowdepth = 0
            self._indata = False

            if self._checkfile is not None: self._checktypes = {}
            else: self._checktypes = None
            self._checkfields = None

            with open(fileName) as file:
                while True:
                    self._rows = []
                    data = file.read(self.blocksize)
                    try:
                        parser.Parse(data)
                    except xml.parsers.expat.ExpatError as err:
                        raise BadlyFormattedInputData("XML reader encountered an error: %s" % str(err))

                    for record in self._rows:
                        if self._checkfile is not None:
                            if self._checkfields is not None:
                                _checktypes = dict((f, "string") for f in self._checkfields)
                                _checktypes.update(self._checktypes)
                                yield self._checkfields, _checktypes
                            else:
                                yield record.keys(), self._checktypes

                        elif self.types is not None:
                            for f in self.fields:
                                try:
                                    record[f] = self._types[f](record[f])
                                except KeyError:
                                    pass
                        yield record

                    if len(data) < self.blocksize:
                        break

    def initialize(self):
        self._iterator = iter(self)

    def next(self):
        try:
            self._got = next(self._iterator)
        except AttributeError:
            return UninitializedError("XMLStream initialize() must be called before next()")

    def flush(self):
        self._got = {}

    def get(self, field):
        try:
            return self._got.get(field, MISSING)
        except AttributeError:
            raise UninitializedError("XMLStream next() must be called before get()")

class XMLStreamFromStream(XMLStream):
    def __init__(self, stream, types, blocksize=4096):
        self.stream = stream
        self.types = types
        self.blocksize = blocksize
        self.fields = set()

        if isinstance(self.types, dict) and all(map(callable, self.types.values())):
            self._types = self.types
            self._autoUpdateTypes = False
        else:
            self._types = {}
            self._autoUpdateTypes = True

    def __iter__(self):
        self._checktypes = None
        parser = self.setupParser()

        self._depth = 0
        self._rowtag = None
        self._rowflatten = None
        self._rowdepth = 0
        self._indata = False

        while True:
            self._rows = []
            data = self.stream.read(self.blocksize)
            try:
                parser.Parse(data)
            except xml.parsers.expat.ExpatError as err:
                raise BadlyFormattedInputData("XML reader encountered an error: %s" % str(err))

            for record in self._rows:
                if len(set(record.keys()).difference(self.fields)) > 0:
                    self.fields = self.fields.union(record.keys())
                    if self._autoUpdateTypes:
                        self._types = dict([(f, cast[t]) for f, t in (self.types.items() if self.types is not None else [(f, "string") for f in self.fields])])

                if self.types is not None:
                    for f in self.fields:
                        if f in record:
                            record[f] = self._types[f](record[f])

                yield record

            if len(data) < self.blocksize:
                break

class NABStream(object):
    def __init__(self, fileLocation, sorter=None, blocksize=4096):
        self.fileNames = getfiles(fileLocation, sorter)
        if len(self.fileNames) == 0:
            raise IOError("No files match \"%s\" (even with wildcards)" % fileLocation)

        self.fields = None
        self.types = None
        self.args = {}
        self.strings = {}

        for fileName in self.fileNames:
            tmpfile = open(fileName, "rb")
            header = tmpfile.readline().rstrip()
            tmpfile.close()

            headerfields = header.decode("utf-8").split()
            if headerfields[0] != "RecArray":
                raise BadlyFormattedInputData("NAB file \"%s\" does not begin with 'RecArray'" % fileName)

            self.args[fileName] = dict(asciistr(f).split("=") for f in headerfields[1:])

            if "masktype" in self.args.keys():
                raise NotImplementedError("No support yet for NAB files (such as \"%s\") with masked NumPy arrays" % fileName)

            if set(self.args[fileName].keys()) != set(["formats", "names"]):
                raise BadlyFormattedInputData("NAB file \"%s\" headers are %s, rather than set([\"formats\", \"names\"])" % (fileName, str(set(self.args[fileName].keys()))))

            thisfields = self.args[fileName]["names"].split(",")
            thistypes = self.args[fileName]["formats"].split(",")
            for i in xrange(len(thistypes)):
                if thistypes[i][0] == "a":
                    thistypes[i] = "string"
                    self.strings[thisfields[i]] = True
                else:
                    self.strings[thisfields[i]] = False

            if self.fields is None:
                self.fields = thisfields
                self.types = thistypes
            else:
                if self.fields != thisfields:
                    raise IncompatibleFilesInChain("NAB file \"%s\" header has fields %s, which differ from the first %s" % (fileName, str(thisfields), str(self.fields)))
                if self.types != thistypes:
                    raise IncompatibleFilesInChain("NAB file \"%s\" header has types %s, which differ from the first %s" % (fileName, str(thistypes), str(self.types)))

            self.args[fileName]["shape"] = blocksize

        self.types = dict(zip(self.fields, self.types))

    def __iter__(self):
        for fileName in self.fileNames:
            with open(fileName, "rb") as file:
                file.readline()

                done = False
                while not done:
                    try:
                        data = numpy.rec.fromfile(file, **self.args[fileName])
                    except ValueError:
                        args = dict(self.args[fileName])
                        del args["shape"]
                        data = numpy.rec.fromfile(file, **args)
                        done = True

                    for i in xrange(len(data)):
                        yield data[i]

    def initialize(self):
        self._iterator = iter(self)

    def next(self):
        try:
            self._got = dict([(f, r) for f, r in zip(self.fields, next(self._iterator))])
        except AttributeError:
            raise UninitializedError("NABStream initialize() must be called before next()")

    def flush(self):
        self._got = {}

    def get(self, field):
        try:
            return self._got.get(field, MISSING)
        except AttributeError:
            raise UninitializedError("NABStream next() must be called before get()")
