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

"""This module defines the Toolkit class."""

class Toolkit(object):
    """A Toolkit is an abstract base class for a collection of methods
    that would be useful for interactive work.

    All methods in the Toolkit may share state, but method names can
    be imported into the local namespace like C{from module import *}
    or they may be kept in a Toolkit object like C{import module as m}.

    @type importables: list of strings
    @param importables: Method names that can be imported.
    """

    importables = []

    usedids = set()

    def __init__(self, **arguments):
        """Initialize the Toolkit.

        @param **arguments: Keywords in the constructor become members
        of the Toolkit instance.
        """

        for name, value in arguments.items():
            setattr(self, name, value)

    def importAll(self, namespace):
        """Import all methods into a namespace.

        This is the equivalent of C{from module import *}.

        @type namespace: dict
        @param namespace: A namespace dictionary, usually C{globals()}.
        """

        for name in self.importables:
            self.importOne(name, namespace)

    def importOne(self, name, namespace):
        """Import one method into a namespace.

        This is the equivalent of C{from module import method}.

        @type name: string
        @param name: The name of the requested method.
        @type namespace: dict
        @param namespace: A namespace dictionary, usually C{globals()}.
        @raise ImportError: If C{name} does not correspond to a known method (in C{importables}), then an error is raised.
        """

        if name in self.importables:
            namespace[name] = getattr(self, name)
        else:
            raise ImportError("\"%s\" is not provided by this Toolkit")
