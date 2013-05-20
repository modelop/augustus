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

"""This module defines the XmlBinding class."""

import sys
import os
import codecs

from lxml.etree import tostring, ElementBase, ElementTree

class XmlBinding(ElementBase):
    """Base class of all in-memory XML representations in Augustus.

    Inherits from lxml's C{Element} class, which is a generalization
    of a Standard Library C{etree} C{Element}.

    C{XmlBinding} adds conveniences for working interactively, such as
    tree-indexes, C{look}, and C{childrenOfTag}/C{childrenOfClass}.
    C{XmlBinding} can also be serialized to and from JSON.

    @see: U{lxml Element reference <http://lxml.de/api/lxml.etree._Element-class.html>}
    @see: U{etree Element reference <http://docs.python.org/release/2.7/library/xml.etree.elementtree.html#element-objects>}
    """

    ### tags, prefixes, and namespaces

    @staticmethod
    def tagToPrefix(elem, nsmap=None):
        """Express the namespace, tagname pair using a prefix instead
        of Clark notation.

        Note that C{elem.tag} expresses the namespace, tagname pair in
        Clark notation.

        @type elem: XmlBinding
        @param elem: The element to report.
        @type nsmap: dict or None
        @param nsmap: The prefix-to-namespace mapping.
        @rtype: string
        @return: The prefix, tagname pair in "prefix:tagname" form.
    """

        if nsmap is None:
            nsmap = elem.nsmap

        if elem.prefix is None:
            prefix = ""
        else:
            prefix = elem.prefix + ":"
        return elem.tag.replace("{%s}" % nsmap[elem.prefix], prefix)

    @property
    def t(self):
        """The namespace, tagname pair expressed using a prefix
        instead of Clark notation.

        Note that {tag} is the namespace, tagname pair in Clark notation.

        @rtype: string
    """

        return self.tagToPrefix(self)

    def hasTag(self, tag):
        """Determine if the element has tag C{tag} without specifying
        the namespace.

        The namespace is derived from the element's internal C{nsmap},
        which was specified as C{xmlns:prefix} in the XML document.

        @type tag: string
        @param tag: The tag to check, without namespace qualification.
        @rtype: bool
        @return: True if the element has tag C{tag}, False otherwise.
        """

        return self.tag == "{%s}%s" % (self.nsmap[self.prefix], tag)

    ### find subelements

    def childrenOfTag(self, tag, iterator=False, reversed=False):
        """Return direct subelements of this element (children, not
        descendants) that match a given tag name.

        @type tag: string
        @param tag: The tag name to match.
        @type iterator: bool
        @param iterator: If True, return an iterator; if False, return a list.
        @type reversed: bool
        @param reversed: If True, return the results in reversed order.
        @rtype: list of XmlBindings
        @return: A list of subelements that can be an empty list.
        """

        tag = "{%s}%s" % (self.nsmap[self.prefix], tag)
        output = self.iterchildren(tag, reversed=reversed)
        if iterator:
            return output
        else:
            return list(output)

    def childOfTag(self, tag, require=False):
        """Return a direct subelement of this element (child, not
        descendant) that matches a given tag name.

        @type tag: string
        @param tag: The tag name to match.
        @type require: bool
        @param require: If True, raise an error when no tags match. If False, that case returns None.
        @rtype: XmlBinding
        @return: The matching subelement or None.
        @raise LookupError: If C{require} is False, an error is raised when the number of matching children is not 0 or 1.  If C{require} is True, an error is raised when the number is not exactly 1.
        """

        output = self.childrenOfTag(tag)
        if len(output) == 0 and not require:
            return None
        elif len(output) == 1:
            return output[0]
        else:
            raise LookupError("There are %d children of %r with tag \"%s\"" % (len(output), self, tag))

    def childrenOfClass(self, cls, iterator=False, reversed=False):
        """Return direct subelements of this element (children, not
        descendants) that match a given XmlBinding subclass.

        @type cls: XmlBinding subclass
        @param cls: The class to match.
        @type iterator: bool
        @param iterator: If True, return an iterator; if False, return a list.
        @type reversed: bool
        @param reversed: If True, return the results in reversed order.
        @rtype: list of XmlBindings
        @return: A list of subelements that can be an empty list.
        """

        output = (x for x in self.iterchildren(reversed=reversed) if isinstance(x, cls))
        if iterator:
            return output
        else:
            return list(output)

    def childOfClass(self, cls, require=False):
        """Return a direct subelement of this element (child, not
        descendant) that matches a given tag name.

        @type cls: XmlBinding subclass
        @param cls: The class to match.
        @type require: bool
        @param require: If True, raise an error when no tags match. If False, that case returns None.
        @rtype: XmlBinding
        @return: The matching subelement or None.
        @raise LookupError: If C{require} is False, an error is raised when the number of matching children is not 0 or 1.  If C{require} is True, an error is raised when the number is not exactly 1.
        """

        output = self.childrenOfClass(cls)
        if len(output) == 0 and not require:
            return None
        elif len(output) == 1:
            return output[0]
        else:
            raise LookupError("There are %d children of %r with class %r" % (len(output), self, cls))

    ### human-readable inspection

    def __repr__(self):
        """An informative representation of the element, used in
        C{look}.

        Although it is surrounded in angle brackets (Python
        convention), this is not an XML representation of the element.

        @rtype: string
        """

        def truncate(x):
            y = repr(x.strip())[:30]
            if len(x) > 30:
                y += "..."
            return y

        attributes = " ".join(["%s=%s" % (n, truncate(v)) for n, v in self.items()])
        nChildren = len(self)
        if nChildren == 0:
            children = ""
        elif nChildren == 1:
            children = " (1 subelement)"
        else:
            children = " (%d subelements)" % nChildren

        if self.text is not None:
            text = truncate(self.text)
            if attributes == "":
                attributes = text
            else:
                attributes += " " + text

        if attributes == "":
            return "<%s%s at 0x%x />" % (self.t, children, id(self))
        else:
            return "<%s %s%s at 0x%x />" % (self.t, attributes, children, id(self))

    def look(self, xpath=None, prefix=None, namespaces=None, stream=None, indexWidth=20):
        """An informative representation of the element and its deeply
        nested subelements, intended for interactive use.

        The output typically goes to a console for a user to read.
        It has two columns: on the left are tree-indexes that can be
        used to extract the XML descendant; on the right are
        C{__repr__} representations of the descendants themselves.

        @type xpath: string or None
        @param xpath: An optional XPath used to filter the output, particularly useful for large XML trees.  See PmmlBinding.xpath for more on how this is interpreted.
        @type prefix: dict
        @param prefix: A simplified prefix-to-namespace map used by PmmlBinding.xpath.  The default prefix for PmmlBindings is "pmml".
        @type namespaces: dict
        @param namespaces: A general prefix-to-namespace map used by any XmlBinding.
        @type stream: file-like object or None
        @param stream: If None, print to C{sys.stdout}; otherwise, write to the specified stream.
        @type indexWidth: int
        @param indexWidth: Number of characters to reserve for the tree-index column.
        @rtype: None
        @return: None; human-readable output is written to the console or a specified stream.
        """

        if stream is None:
            stream = sys.stdout

        if xpath is not None:
            kwds = {}
            if prefix is not None: kwds["prefix"] = prefix
            if namespaces is not None: kwds["namespaces"] = namespaces
            xpath = self.xpath(xpath, **kwds)

        def iterator(treeindex, element):
            yield treeindex, element
            for index, child in enumerate(element):
                if isinstance(child, ElementBase):
                    for ti, c in iterator(treeindex + (index,), child):
                        yield ti, c

        for treeindex, descendant in iterator(tuple(), self):
            if xpath is None or descendant in xpath:
                stream.write("%s %s%s%s" % (("%%-%ds" % indexWidth) % repr(treeindex), ". . " * len(treeindex), repr(descendant), os.linesep))
                stream.flush()

    ### copying and serialization

    @staticmethod
    def toJsonDict(elem, topLevel=True, number=None):
        """Serialize an element C{elem} to JSON, rather than XML.

        There is no standard XML-to-JSON specification, so we define
        our own.  Our specification is very similar to U{this proposal<http://www.xml.com/pub/a/2006/05/31/converting-between-xml-and-json.html>},
        which collects subelements of different tagnames into
        different JSON lists, rather than having one long list and
        needing to specify the tag of each element in that list.  This
        has the following advantages, particularly useful for PMML:
          - Frequent tagnames (like <Segment>) are not repeated,
            wasting space.
          - Subelements with a given tagname can be quickly queried,
            without having to iterate over a list that contains
            non-matching tagnames.
        It has the following disadvantages:
          - The relative order of subelements with different tagnames
            is not preserved.
        We therefore additionally include a JSON attribute named "#"
        to specify the ordering of subelements in the XML
        representation.  Also, the specification referenced above
        represents single-child subelements as JSON objects and
        multiple children as JSON lists, but for consistency and ease
        of parsing, we always use lists.  The last difference is that
        we include "#tail" as well as "#text", so that text outside of
        an element is preserved (rarely relevant for PMML, but
        included for completeness).

        Note that this method returns a JSON-like dictionary, not a
        string.  To serialize to JSON, use the C{json} module from the
        Python Standard Library, a faster variant, or an exotic
        serializer such as BSON.

        @type elem: XmlBinding
        @param elem: The XmlBinding tree to represent as a JSON-like dictionary.
        @type topLevel: bool
        @param topLevel: If True, include the prefix-to-namespace map once at the top of the JSON document.  A user would rarely change this from its default value; it is included so that the function can work recursively without producing multiple namespace maps.
        @type number: int or None
        @param number: Number to include as a "#" attribute.  A user would rarely change this from its default value; it is used in recursion.
        @rtype: dict
        @return: Nested Python dictionaries and lists, to be passed to the C{json} module or other serializer.
        """

        item = dict(("@" + key, value) for (key, value) in elem.attrib.items())

        item["#"] = number

        text, tail = elem.text, elem.tail
        if text is not None:
            text = text.strip()
            if text != "":
                item["#text"] = text
        if tail is not None:
            tail = tail.strip()
            if tail != "":
                item["#tail"] = tail

        for i, subelem in enumerate(elem.iterchildren()):
            subjson = XmlBinding.toJsonDict(subelem, False, i)
            subtag = XmlBinding.tagToPrefix(subelem, elem.nsmap)
            try:
                value = item[subtag]
                value.append(subjson)
            except KeyError:
                item[subtag] = [subjson]

        if topLevel:
            nsmap = elem.nsmap
            if None in nsmap:
                nsmap[""] = nsmap[None]
                del nsmap[None]
            return {XmlBinding.tagToPrefix(elem): [item], "#nsmap": nsmap}
        else:
            return item

    def jsonDict(self):
        """Serialize this element to JSON, rather than XML.

        @see: C{toJsonDict}, the static version of this method.
        """

        return self.toJsonDict(self)

    @staticmethod
    def toXml(elem, *args, **kwds):
        """Serialize an element to an XML string.

        @type elem: XmlBinding
        @param elem: The XmlBinding tree to represent as an XML string.
        @param *args, **kwds: Arguments passed to lxml's U{tostring<http://lxml.de/api/lxml.etree-module.html#tostring>}
        """

        return tostring(elem, *args, **kwds)

    def xml(self, *args, **kwds):
        """Serialize this element to an XML string.

        @see: C{toXml}, the static version of this method.
        """

        return self.toXml(self, *args, **kwds)

    @staticmethod
    def toXmlFile(elem, file, *args, **kwds):
        """Serialize and write an element to an XML file.

        Note that lxml's C{compression} argument can be used to set a
        Gzip compression level from 1 (fastest) to 9 (smallest file),
        and that ModelLoader.loadXml can read Gzip-compressed files.
        
        @type elem: XmlBinding
        @param elem: The XmlBinding tree to represent as an XML string.
        @type file: file-like object
        @param file: The file or stream to write.
        @param *args, **kwds: Arguments passed to lxml's U{ElementTree.write<http://lxml.de/api/lxml.etree._ElementTree-class.html#write>}
        """

        if isinstance(file, basestring):
            file = codecs.open(file, "w", "utf-8")
        ElementTree(elem).write(file, *args, **kwds)

    def xmlFile(self, file, *args, **kwds):
        """Serialize and write this element to an XML file.

        @see: C{toXmlFile}, the static version of this method.
        """

        self.toXmlFile(self, file, *args, **kwds)

    @staticmethod
    def toXmlFileCanonical(elem, file, *args, **kwds):
        """Serialize and write an element to an XML file using lxml's C14N method.

        @see: All arguments are the same as C{toXmlFile}.
        """

        ElementTree(elem).write_c14n(file, *args, **kwds)

    def xmlFileCanonical(self, file, *args, **kwds):
        """Serialize and write this element to an XML file using lxml's C14N method.

        @see: All arguments are the same as C{xmlFile}.
        """

        self.toXmlFile(self, file, *args, **kwds)

    ### access via tree-indexes

    def root(self):
        """Return the root of the tree.

        @rtype: XmlBinding
        @return: The node of the tree that does not have a parent.
        Note that this has the same data type as any other element in
        the tree; it is not a W3C DOM owner document.
        """

        pointer = self
        while pointer.getparent() is not None:
            pointer = pointer.getparent()
        return pointer

    def treeindex(self):
        """Return the tree-index of this element, relative to the root
        of the tree.
        
        Tree-indexes are tuples of 0-based indexes that represent
        descendants in a deeply nested tree.  For instance, C{elem[2, 1, 5]}
        represents the sixth child of the second child of the
        third child of C{elem}.  It is equivalent to C{elem[2][1][5]}
        but easier to type.  Attributes are represented by
        tree-indexes terminating with a string: C{elem[2, 1, 5, "attr"]}
        is equivalent to C{elem[2][1][5]["attr"]}.  Tree-indexes are always
        relative and can never reference self or ancestors.

        @rtype: tuple of int
        @return: The tree-index of this element, which can never terminate with a string because XmlBindings only represent element nodes, not attribute nodes.
        """

        output = []
        one = self
        two = self.getparent()
        while two is not None:
            output.append(two.index(one))
            one = two
            two = one.getparent()
        output.reverse()
        return tuple(output)

    def __getitem__(self, x):
        """Return the subelement at a given index or tree-index,
        subelements in a slice, or the attribute value at a given
        tree-index or attribute name.
        
        Note that attributes are always strings, even if the XSD
        guarantees that they have the form of an int or float.

        @type x: int, tree-index, slice, string
        @param x: The location of the desired object or objects.
        @rtype: XmlBinding, list of XmlBindings, or string
        @return: The desired object or objects.
        @see: C{treeindex} for an explanation of tree-indexes.
        """

        try:
            return super(XmlBinding, self).__getitem__(x)
        except TypeError:
            if isinstance(x, basestring):
                return self.get(x)
            elif isinstance(x, tuple) and len(x) > 1:
                return self[x[0]][x[1:]]
            elif isinstance(x, tuple) and len(x) == 1:
                return self[x[0]]
            else:
                raise

    def __setitem__(self, x, value):
        """Change the subelement at a given index or tree-index,
        subelements in a slice, or the attribute value at a given
        tree-index or attribute name.

        Note that attributes are always strings, even if the XSD
        guarantees that they have the form of an int or float.

        @type x: int, tree-index, slice, string
        @param x: The location of the desired object or objects.
        @type value: XmlBinding, list of XmlBindings, or string
        @param value: The desired object or objects.
        @see: C{treeindex} for an explanation of tree-indexes.
        """

        try:
            output = super(XmlBinding, self).__setitem__(x, value)
        except TypeError:
            if isinstance(x, basestring):
                self.set(x, value)
            elif isinstance(x, tuple) and len(x) > 1:
                self[x[0]][x[1:]] = value
            elif isinstance(x, tuple) and len(x) == 1:
                self[x[0]] = value
            else:
                raise

    def __delitem__(self, x):
        """Delete the subelement at a given index or tree-index,
        subelements in a slice, or the attribute value at a given
        tree-index or attribute name.

        @type x: int, tree-index, slice, string
        @param x: The location of the desired object or objects.
        @see: C{treeindex} for an explanation of tree-indexes.
        """

        try:
            super(XmlBinding, self).__delitem__(x)
        except TypeError:
            if isinstance(x, basestring):
                del self.attrib[x]
            elif isinstance(x, tuple) and len(x) > 1:
                del self[x[0]][x[1:]]
            elif isinstance(x, tuple) and len(x) == 1:
                del self[x[0]]
            else:
                raise
