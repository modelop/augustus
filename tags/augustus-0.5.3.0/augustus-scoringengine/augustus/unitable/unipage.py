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

from augustus.core.python3transition import *

import sys
import mmap
import os
import itertools
try:
    import cPickle as pickle
except ImportError:
    import pickle
try:
    from cStringIO import StringIO as BytesIO
except ImportError:
    try:
        from StringIO import StringIO as BytesIO
    except ImportError:
        from io import BytesIO

try:
    from itertools import izip as zip
except ImportError:
    pass

import numpy

from augustus.unitable.utilities import typeToDtype
import augustus.unitable.xtbl10 as xtbl
from augustus.unitable.evaluate import FieldExpression

class BeyondPageException(Exception): pass

class UniPage(object):
    emptyString = str("").encode("utf-8")
    comma = str(",")

    def load(self, fields=None): pass

    def release(self): pass

    def __init__(self, fields, types):
        self.fields = fields
        self._fields = dict((field, i) for i, field in enumerate(fields))
        self.types = types

    def initMemory(self, allocation):
        self.allocation = allocation
        self.data = [numpy.empty(allocation, dtype=typeToDtype.get(self.types[i], self.types[i])) if self.types[i] != "object" else [None]*allocation for i in xrange(len(self.fields))]
        self.categories = [({}, {}) if self.types[i] == "category" else None for i in xrange(len(self.fields))]
        self.buffers = [BytesIO() if self.types[i] == "string" else None for i in xrange(len(self.fields))]
        self.length = 0

    def initExisting(self, allocation, values, copy=True, stringToCategory=True, stringToBuffer=True, loadFields=None):
        for name, arr in values.items():
            if len(arr) != allocation:
                raise ValueError("pre-existing array \"%s\" must have allocation %d, not %d" % (name, allocation, len(arr)))

        self.allocation = self.length = allocation

        if not hasattr(self, "data") or loadFields is None:
            if loadFields is None: loadFields = self.fields

            self.data = [None] * len(self.fields)
            self.categories = [None] * len(self.fields)
            self.buffers = [None] * len(self.fields)

        for i, field in enumerate(self.fields):
            if field in loadFields:
                arr = values[field]
                if len(arr) != allocation:
                    raise ValueError("length of field \"%s\" is %d, but allocating %d" % (field, len(arr), allocation))

                if stringToCategory and self.types[i] == "category":
                    try:
                        uniqueValues, indicies = numpy.unique(arr, return_inverse=True)
                    except TypeError:
                        if map(int, numpy.__version__.split(".")) < [1, 3, 0]:
                            indicies, uniqueValues = numpy.unique1d(arr, return_inverse=True)
                        else:
                            uniqueValues, indicies = numpy.unique1d(arr, return_inverse=True)

                    v_to_n = dict((v, n) for n, v in enumerate(uniqueValues))
                    n_to_v = dict((n, v) for n, v in enumerate(uniqueValues))

                    self.data[i] = indicies
                    self.categories[i] = (v_to_n, n_to_v)

                elif self.types[i] == "string":
                    if stringToBuffer:
                        buf = BytesIO()
                        arr2 = numpy.empty(allocation, dtype=typeToDtype.get(self.types[i], self.types[i]))
                        for j, v in enumerate(arr):
                            buf.write(str(v).encode("utf-8"))
                            arr2[j] = buf.tell()
                    else:
                        arr2 = arr
                        buf = None

                    self.data[i] = arr2
                    self.buffers[i] = buf

                elif self.types[i] == "object":
                    if copy or isinstance(arr, numpy.ndarray):
                        arr = list(arr)

                    self.data[i] = arr

                else:
                    if copy or not isinstance(arr, numpy.ndarray):
                        arr = numpy.array(arr, dtype=typeToDtype.get(self.types[i], self.types[i]))
                    self.data[i] = arr

    def fill(self, values):
        if self.length >= self.allocation: raise BeyondPageException

        for i, v in enumerate(values):
            if self.categories[i] is not None:
                v = str(v)
                v_to_n, n_to_v = self.categories[i]
                if v not in v_to_n:
                    n = len(v_to_n)
                    v_to_n[v] = n
                    n_to_v[n] = v
                else:
                    n = v_to_n[v]

                self.data[i][self.length] = n

            elif self.buffers[i] is not None:
                self.buffers[i].write(str(v).encode("utf-8"))
                self.data[i][self.length] = self.buffers[i].tell()

            else:
                self.data[i][self.length] = v

        self.length += 1

    def evaluate(self, exprTree, fields, lookups, categoryToString=True):
        arrays = {}
        for field in fields:
            arrays[field] = self.getcolumn(field, categoryToString=False)
        output = [v.calculate(arrays, lookups) for v in exprTree]

        # single-value output (expression was only constants) into an array with the proper length
        for i in xrange(len(output)):
            if not isinstance(output[i], (list, tuple, numpy.ndarray)):
                if isinstance(output[i], basestring):
                    output[i] = numpy.fromiter((output[i] for x in xrange(self.length)), dtype="|S%d" % len(output[i]))
                else:
                    value = output[i]
                    output[i] = numpy.ones(self.length, dtype=type(value))
                    numpy.multiply(output[i], value, output[i])

        if categoryToString:
            for i, expr in enumerate(exprTree):
                if isinstance(expr, FieldExpression) and self.types[self.fields.index(expr.name)] == "category":
                    n_to_v = self.categories[self.fields.index(expr.name)][1]
                    values = numpy.array([n_to_v[j] for j in xrange(len(n_to_v))])
                    output[i] = list(values[output[i]])

        return output

    def getitem(self, field, index, categoryToString=True):
        if index >= self.length: raise BeyondPageException
        self.load([field])

        i = self._fields[field]
        if categoryToString and self.categories[i] is not None:
            return self.categories[i][1][self.data[i][index]]

        elif self.buffers[i] is not None:
            if index == 0:
                start, end = 0, self.data[i][0]
            else:
                start, end = self.data[i][index - 1], self.data[i][index]

            if isinstance(self.buffers[i], numpy.ndarray):
                return self.emptyString.join(self.buffers[i][start:end]).decode("utf-8")
            else:
                return self.buffers[i].getvalue()[start:end].decode("utf-8")

        else:
            return self.data[i][index]

    def getcolumn(self, field, categoryToString=True):
        self.load([field])

        i = self._fields[field]
        if categoryToString and self.categories[i] is not None:
            n_to_v = self.categories[i][1]
            values = numpy.array([n_to_v[j] for j in xrange(len(n_to_v))])

            if self.length == self.allocation:
                return list(values[self.data[i]])
            else:
                return list(values[self.data[i][:self.length]])

        elif self.buffers[i] is not None:
            if self.length == self.allocation:
                ends = self.data[i]
            else:
                ends = self.data[i][:self.length]
            starts = numpy.roll(ends, 1)
            starts[0] = 0
            if isinstance(self.buffers[i], numpy.ndarray):
                buf = self.buffers[i]
                return [self.emptyString.join(buf[start:end]).decode("utf-8") for start, end in zip(starts, ends)]
            else:
                buf = self.buffers[i].getvalue()
                return [buf[start:end].decode("utf-8") for start, end in zip(starts, ends)]

        else:
            if self.length == self.allocation:
                return self.data[i]
            elif self.types[i] == "object":
                return itertools.islice(self.data[i], self.length)
            else:
                return self.data[i][:self.length]

    def getrow(self, index, categoryToString=True):
        if index >= self.length: raise BeyondPageException
        self.load()

        if not categoryToString:
            return [self.data[i][index] for i in xrange(len(self.fields))]

        output = []
        for i in xrange(len(self.fields)):
            if self.categories[i] is not None:
                output.append(self.categories[i][1][self.data[i][index]])

            elif self.buffers[i] is not None:
                if index == 0:
                    start, end = 0, self.data[i][0]
                else:
                    start, end = self.data[i][index - 1], self.data[i][index]
                if isinstance(self.buffers[i], numpy.ndarray):
                    output.append(self.emptyString.join(self.buffers[i][start:end]).decode("utf-8"))
                else:
                    output.append(self.buffers[i].getvalue()[start:end].decode("utf-8"))

            else:
                output.append(self.data[i][index])

        return output

    def write_CSV(self, file):
        self.load()

        columns = [self.getcolumn(f) for f in self.fields]
        for row in xrange(self.length):
            file.write(self.comma.join([str(columns[i][row]) for i in xrange(len(self.fields))]))
            file.write(os.linesep)

    def write_XML(self, file):
        self.load()

        columns = [self.getcolumn(f) for f in self.fields]
        for row in xrange(self.length):
            file.write("<row>")
            file.write("".join(["<%s>%s</%s>" % (f, str(columns[i][row]), f) for i, f in enumerate(self.fields)]))
            file.write("</row>")
            file.write(os.linesep)

    def write_NAB(self, file, formats):
        self.load()

        tmpcopy1 = [self.getcolumn(f) for f in self.fields]
        for i, arr in enumerate(tmpcopy1):
            if self.types[i] in ("category", "string"):
                tmpcopy1[i] = numpy.array([x.encode("utf-8") for x in tmpcopy1[i]], dtype=formats[i])

        tmpcopy2 = numpy.rec.array(tmpcopy1, names=self.fields, formats=formats)
        tmpcopy2.tofile(file)

        del tmpcopy1
        del tmpcopy2

    def write_XTBL(self, file, footer):
        self.load()

        pageFieldOffsets = []

        for i, field in enumerate(self.fields):
            pageFieldOffsets.append(xtbl.PageFieldOffset(field, file.tell()))

            if self.types[i] == "object":
                footer.fields[field].attrib["dtype"] = "pickle"
                if self.length == self.allocation:
                    pickle.dump(self.data[i], file)
                else:
                    pickle.dump(self.data[i][:self.length], file)

            elif self.types[i] == "string":
                footer.fields[field].attrib["dtype"] = "%s+str" % str(self.data[i].dtype)
                if self.length == self.allocation:
                    file.write(self.data[i].data)
                else:
                    file.write(self.data[i][:self.length].data)
                if isinstance(self.buffers[i], numpy.ndarray):
                    self.buffers[i].tofile(file)
                else:
                    file.write(self.buffers[i].getvalue())

            else:
                footer.fields[field].attrib["dtype"] = str(self.data[i].dtype)
                if self.length == self.allocation:
                    file.write(self.data[i].data)
                else:
                    file.write(self.data[i][:self.length].data)

            if self.types[i] == "category":
                footer.lookups[field].n_to_v = self.categories[i][1]

        footer.pages.children.append(xtbl.Page(self.length, pageFieldOffsets))

class UniPageDiskCacheManager(object):
    def __init__(self, limitGB, memoryMap):
        if limitGB is None:
            self.limitBytes = None
        else:
            self.limitBytes = int(round(limitGB * 1024 * 1024 * 1024))

        self.memoryMap = memoryMap

        self.bytesInMemory = {}
        self.releaseFunctions = {}
        self.order = []

    def addBytes(self, me, bytes):
        self.bytesInMemory[me] += bytes

        if self.limitBytes is not None:
            try:
                del self.order[self.order.index(me)]
            except ValueError:
                pass
            self.order.append(me)
            
            totalBytes = sum(self.bytesInMemory.values())
            if totalBytes > self.limitBytes:
                for i in self.order:
                    if i != me:
                        self.bytesInMemory[i] = 0
                        self.releaseFunctions[i]()
                        totalBytes = sum(self.bytesInMemory.values())
                        if totalBytes <= self.limitBytes:
                            break
        
class UniPageOnDisk(UniPage):
    def initDisk(self, allocation, fileName, byteOffsets, dtypes, categories, uniPageDiskCacheManager):
        self.allocation = self.length = allocation
        self.fileName = fileName
        self.byteOffsets = byteOffsets
        self.dtypes = dtypes
        self.categories = self._categories = categories

        self._fieldsSet = set()
        self.uniPageDiskCacheManager = uniPageDiskCacheManager
        self.uniPageDiskCacheManager.bytesInMemory[id(self)] = 0
        self.uniPageDiskCacheManager.releaseFunctions[id(self)] = self.release
        self.uniPageDiskCacheManager.order.append(id(self))

        if sys.version_info[:2] >= (2,6):
            self._memmap = self._memmapNew
        else:
            self._memmap = self._memmapOld

    def _memmapNew(self, offset, bytes):
        nudge = offset % mmap.ALLOCATIONGRANULARITY
        start = offset - nudge
        offset -= start
        bytes += nudge
        mm = mmap.mmap(self._file.fileno(), bytes, access=mmap.ACCESS_COPY, offset=start)
        return mm, offset

    def _memmapOld(self, offset, bytes):
        mm = mmap.mmap(self._file.fileno(), 0, access=mmap.ACCESS_COPY)
        return mm, offset

    def load(self, fields=None):
        if not hasattr(self, "data") or (fields is not None and len(set(fields).difference(self._fieldsSet)) > 0):
            if fields is None:
                loadFields = self.fields
                self._fieldsSet = set(self.fields)
            else:
                loadFields = [None] * len(self.fields)
                for f in fields:
                    loadFields[self._fields[f]] = f
                    self._fieldsSet.add(f)

            self._file = open(self.fileName, "rb")

            arrays = {}
            if not hasattr(self, "buffers"):
                buffers = [None] * len(self.fields)
            else:
                buffers = list(self.buffers)
                
            for i, field in enumerate(loadFields):
                if field is None: continue

                if self.types[i] == "object":
                    self._file.seek(self.byteOffsets[field])
                    arrays[field] = pickle.load(self._file)

                elif self.types[i] == "string":
                    if self.uniPageDiskCacheManager.memoryMap:
                        offset = self.byteOffsets[field]
                        bytes = numpy.dtype(self.dtypes[field].rstrip("+str")).itemsize * self.allocation
                        self.uniPageDiskCacheManager.addBytes(id(self), bytes)
                        mm, offset = self._memmap(offset, bytes)
                        arrays[field] = numpy.frombuffer(mm, dtype=self.dtypes[field].rstrip("+str"), count=self.allocation, offset=offset)

                        offset = self.byteOffsets[field] + arrays[field].nbytes
                        bytes = int(arrays[field][-1])
                        self.uniPageDiskCacheManager.addBytes(id(self), bytes)
                        mm, offset = self._memmap(offset, bytes)
                        buffers[i] = numpy.frombuffer(mm, dtype="c", count=arrays[field][-1], offset=offset)

                    else:
                        bytes = numpy.dtype(self.dtypes[field].rstrip("+str")).itemsize * self.allocation
                        self.uniPageDiskCacheManager.addBytes(id(self), bytes)
                        self._file.seek(self.byteOffsets[field])
                        arrays[field] = numpy.fromfile(self._file, dtype=self.dtypes[field].rstrip("+str"), count=self.allocation)

                        bytes = int(arrays[field][-1])
                        self.uniPageDiskCacheManager.addBytes(id(self), bytes)
                        self._file.seek(self.byteOffsets[field] + arrays[field].nbytes)
                        buffers[i] = numpy.fromfile(self._file, dtype="c", count=arrays[field][-1])

                    if self.uniPageDiskCacheManager.limitBytes is not None:
                        arrays[field].flags.writeable = False
                        buffers[i].flags.writeable = False

                else:
                    if self.uniPageDiskCacheManager.memoryMap:
                        offset = self.byteOffsets[field]
                        bytes = numpy.dtype(self.dtypes[field]).itemsize * self.allocation
                        mm, offset = self._memmap(offset, bytes)

                        self.uniPageDiskCacheManager.addBytes(id(self), bytes)
                        arrays[field] = numpy.frombuffer(mm, dtype=self.dtypes[field], count=self.allocation, offset=offset)

                    else:
                        bytes = numpy.dtype(self.dtypes[field]).itemsize * self.allocation
                        self.uniPageDiskCacheManager.addBytes(id(self), bytes)
                        self._file.seek(self.byteOffsets[field])
                        arrays[field] = numpy.fromfile(self._file, dtype=self.dtypes[field], count=self.allocation)

                    if self.uniPageDiskCacheManager.limitBytes is not None:
                        arrays[field].flags.writeable = False

            if not self.uniPageDiskCacheManager.memoryMap:
                self._file.close()
                del self._file

            self.initExisting(self.allocation, arrays, copy=False, stringToCategory=False, stringToBuffer=False, loadFields=loadFields)
            self.categories = self._categories
            self.buffers = buffers

    def release(self):
        if hasattr(self, "data"):
            del self.data
            del self.buffers
            self._fieldsSet = set()

            if self.uniPageDiskCacheManager.memoryMap:
                self._file.close()
                del self._file

    def fill(self, values):
        raise BeyondPageException
