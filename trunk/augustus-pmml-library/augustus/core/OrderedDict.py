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

"""This module defines the OrderedDict class."""

class OrderedDict(dict):
    """OrderedDict provides a base class for ordered dictionaries,
    which are used at many levels in Augustus.

    Python's OrderedDict was added to the collections standard library
    module in version 2.7, but we need to support earlier versions.
    """

    def __init__(self, *args, **kwds):
        super(OrderedDict, self).__init__(*args, **kwds)
        self._order = []
        if len(args) > 0:
            d = args[0]
            if isinstance(d, dict):
                self._order = d.keys()
            else:
                self._order = [name for name, value in d]
        self._order.extend(kwds.keys())

    def __repr__(self):
        return "{" + ", ".join("%r: %r" % (name, value) for name, value in self.iteritems()) + "}"

    def __getstate__(self):
        return {"order": self._order, "values": self.values(), "attributes": self.__dict__}

    def __setstate__(self, serialization):
        self._order = serialization["order"]
        values = serialization["values"]
        setitem = super(OrderedDict, self).__setitem__
        for i, name in enumerate(self._order):
            setitem(name, values[i])

        self.__dict__ = serialization["attributes"]

    def __setitem__(self, name, value):
        # ugly fix for pickle protocol=2 (http://bugs.python.org/issue826897)
        if "_order" not in self.__dict__:
            return

        output = super(OrderedDict, self).__setitem__(name, value)
        if name in self._order:
            self._order.remove(name)
        self._order.append(name)
        return output

    def __delitem__(self, name):
        output = super(OrderedDict, self).__delitem__(name)
        self._order.remove(name)
        return output

    def clear(self):
        output = super(OrderedDict, self).clear()
        self._order = []
        return output

    def pop(self, name):
        output = super(OrderedDict, self).pop(name)
        self._order.remove(name)
        return output

    def popitem(self):
        name, value = super(OrderedDict, self).popitem()
        self._order.remove(name)
        return name, value

    def update(self, dictionary):
        output = super(OrderedDict, self).update(dictionary)
        for key in dictionary.keys():
            if key in self._order:
                self._order.remove(key)
        self._order.extend(keys)
        return output

    def iterkeys(self):
        return (name for name in self._order)

    def itervalues(self):
        return (self[name] for name in self._order)

    def iteritems(self):
        return ((name, self[name]) for name in self._order)

    def keys(self):
        return list(self.iterkeys())

    def values(self):
        return list(self.itervalues())

    def items(self):
        return list(self.iteritems())
