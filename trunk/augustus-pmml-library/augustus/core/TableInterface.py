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

"""This module defines the TableInterface class."""

from lxml.etree import iterparse
from lxml.etree import XMLSyntaxError

class TableInterface(object):
    """TableInterface is the base class for PMML elements that contain
       tables rendered in XML.

       The XML must consist of one level of named tags within a sequence
       of <row> blocks.  For instance::

           <row>
               <fieldName1>field value 1</fieldName1>
               <fieldName2>60</fieldName2>
           </row>
       """

    def iterateOverFile(self, file):
        """Iterate over a table located in a file.

           @type file: string
           @param file: The fileName.
           @rtype: iterator that yields dicts
           @return: Each item from the iterator is a dictionary mapping tagnames to string values.
           """

        try:
            for event, row in iterparse(file, events=("end",), tag="row"):
                output = dict((child.tag, child.text.strip() if child.text is not None else "") for child in row)

                row.clear()
                while row.getprevious() is not None:
                    del row.getparent()[0]

                yield output
        except XMLSyntaxError as err:  # This particular error is a bug in lxml: {libxml2.x86_64 0:2.7.8-9.fc17, libxml2-python.x86_64 0:2.7.8-9.fc17, python-lxml.x86_64 0:3.2.1-1.fc17}
            if err.msg is not None:    # See https://bugs.launchpad.net/lxml/+bug/1185701 and
                raise                  #     https://bugzilla.redhat.com/show_bug.cgi?id=874546

    def iterateOverMemory(self, element):
        """Iterate over a table that has been loaded into memory.

           @type element: lxml.etree.Element
           @param element: The XML-based table to iterate over.
           @rtype: iterator that yields dicts
           @return: Each item from the iterator is a dictionary mapping tagnames to string values.
           """

        namespace = element.nsmap.get(element.prefix)

        if namespace is not None:
            namespace = "{" + namespace + "}"

            for row in element.getiterator(tag="%srow" % namespace):
                yield dict((child.tag.replace(namespace, ""), child.text.strip() if child.text is not None else "") for child in row)

        else:
            for row in element.getiterator(tag="row"):
                yield dict((child.tag, child.text.strip() if child.text is not None else "") for child in row)
