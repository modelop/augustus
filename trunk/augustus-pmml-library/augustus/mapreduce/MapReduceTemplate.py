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

"""This module is a template mapper/reducer to send to Hadoop with modifications."""

BUILTIN_GLOBALS = globals().copy()

import sys
import struct
import marshal
import new
import inspect
import logging
try:
    import cPickle as pickle
except ImportError:
    import pickle

PICKLE_PROTOCOL = 2

def serializeClass(cls):
    objects = {"name": cls.__name__, "nestedClasses": {}, "code": {}, "data": {}}

    def fillObjects(cls, objects):
        for baseClass in reversed(cls.__bases__):
            fillObjects(baseClass, objects)

        for name, item in cls.__dict__.items():
            if name[:2] != "__":
                if inspect.isclass(item):
                    objects["nestedClasses"][name] = serializeClass(item)

                elif callable(item):
                    try:
                        code = item.func_code
                    except AttributeError:
                        code = item.__code__
                    objects["code"][name] = marshal.dumps(code)

                else:
                    objects["data"][name] = item

    fillObjects(cls, objects)

    return pickle.dumps(objects, protocol=PICKLE_PROTOCOL)

def unserializeClass(string, baseClass, overrideAttributes, namespace):
    objects = pickle.loads(string)
    classAttributes = objects["data"]

    for name, nestedClass in objects["nestedClasses"].items():
        classAttributes[name] = unserializeClass(nestedClass)

    for name, code in objects["code"].items():
        classAttributes[name] = new.function(marshal.loads(code), namespace)

    classAttributes.update(overrideAttributes)

    return new.classobj(objects["name"], (baseClass,), classAttributes)

class Controller(object):
    def __init__(self, mapReduceApplication):
        self.mapReduceApplication = mapReduceApplication

    def mapper(self):
        self.mapReduceApplication.beginMapperTask()
        while True:
            header = sys.stdin.read(4)
            if not header: break

            length = struct.unpack("!i", header)[0]
            sys.stdin.read(length)

            tab = sys.stdin.read(1)
            if tab != "\t":
                raise RuntimeError("Incorrectly formatted input from SequenceFile: %r should be a tab" % tab)

            header = sys.stdin.read(4)
            length = struct.unpack("!i", header)[0]

            sys.stdin.read(5)
            dataString = sys.stdin.read(length - 5)

            record = pickle.loads(dataString)

            eoln = sys.stdin.read(1)
            if eoln != "\n":
                raise RuntimeError("Incorrectly formatted input from SequenceFile: %r should be an eoln" % eoln)

            self.mapReduceApplication.mapper(record)

        self.mapReduceApplication.endMapperTask()

    def reducer(self):
        self.mapReduceApplication.beginReducerTask()

        lastkey = None
        while True:
            header = sys.stdin.read(5)
            if not header: break
            typecode, length = struct.unpack("!bi", header)
            if typecode != 0:
                raise RuntimeError("Key should be binary, but typecode is %d" % typecode)

            key = sys.stdin.read(length)

            header = sys.stdin.read(5)
            typecode, length = struct.unpack("!bi", header)
            if typecode != 0:
                raise RuntimeError("Value should be binary, but typecode is %d" % typecode)

            dataString = sys.stdin.read(length)
            value = pickle.loads(dataString)

            if key != lastkey:
                if lastkey is not None:
                    self.mapReduceApplication.endReducerKey(lastkey)
                self.mapReduceApplication.beginReducerKey(key)

            self.mapReduceApplication.reducer(key, value)
            lastkey = key

        if lastkey is not None:
            self.mapReduceApplication.endReducerKey(lastkey)

outputStream = sys.stdout

def emit(appself, key, record):
    if key is None:
        logger.debug("OutputRecord: %r", record)
        outputStream.write(struct.pack("!bi", 0, 0))
    else:
        logger.debug("OutputKeyValue \"%s\": %r", key, record)
        outputStream.write(struct.pack("!bi", 0, len(key)))
        outputStream.write(key)

    dataString = pickle.dumps(record, protocol=PICKLE_PROTOCOL)
    outputStream.write(struct.pack("!bi", 0, len(dataString)))
    outputStream.write(dataString)
