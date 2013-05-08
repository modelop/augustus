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

"""Define an XML DOM, an associated XSD-based validator, and a validate-while-reading function."""

from augustus.core.python3transition import *

import sys
import os
import re
import itertools
import codecs
import warnings
import copy
import xml.sax, xml.sax.handler
import inspect
import string
import traceback
import urllib
from xml.sax.saxutils import escape, quoteattr

try:
    from cStringIO import StringIO as BytesIO
except ImportError:
    try:
        from StringIO import StringIO as BytesIO
    except ImportError:
        from io import BytesIO

try:
    from functools import reduce
except ImportError:
    pass

try:
    from itertools import imap as map
except ImportError:
    pass

from sys import version_info
if version_info >= (2, 6):
    chain_from_iterable = itertools.chain.from_iterable
else:
    def chain_from_iterable(iterables):
        for it in iterables:
            for element in it:
                yield element

class XMLError(Exception): pass           # incorrectly formed XML (user's error)
class XMLValidationError(XMLError): pass  # XML does not fit XSD schema (user's error)
class XSDDefinitionError(XMLError): pass  # XSD schema is invalid (programmer's error)
    
### XML class

class XML(object):
    """Base class for all XML objects.

    Usually contains self.tag (str), self.attrib (dict), and
    self.children (list), but if it is an XMLSpecial subclass (for
    comments, text, etc.), then it contains only self.text (str).

    If the XML contains self.value, it will be serialized as::

        <tag>value</tag>

    where tag is self.tag and value is str(self.value).

    In short, quick code, access children and attributes deeply
    with tree-indexes::

        xmlobj[0, 5, 3, "something"]

    is equivalent to::

        xmlobj.children[0].children[5].children[3].attrib["something"]

    In production-level code, it is safer to not access children
    by indexes at all and to avoid tree-indexes because its
    recursive calls are slow.

    Indexes include all objects, including XMLSpecial objects like
    XMLText nodes and comments.  To loop over children without
    XMLSpecial objects, use the implicit iterator::

        for child in xmlobj:
            child is not XMLSpecial
    """

    xsd = None
    xsdType = None
    xsdGroup = None
    classMap = None

    ### declare an XML element using Pythonized keywords
    def _preprocess_attribname(self, name):
        if name == "__TAG__": name = "tag"
        name = name.replace("__", ":")
        name = name.replace("_", "-")
        return name
    
    def __init__(self, tag, *children, **attrib):
        """Create a new XML object with a given tag, children, and
        attributes.

        Note that attributes containing '-' and ':' can be expressed
        as keyword arguments by these translations:

            '-'   becomes   '_'
            ':'   becomes   '__'

        (Bypasses problems with Python syntax.)
        """

        self.tag = tag
        self.attrib = {}
        for name, value in attrib.items():
            self.attrib[self._preprocess_attribname(name)] = value
        self.children = []
        for child in children:
            if isinstance(child, basestring):
                self.children.append(XMLText(child))
            else:
                self.children.append(child)

    ### access attributes and children with a treeindex
    def _treeindex_descend(self, obj, treeindex):
        if isinstance(treeindex, (list, tuple)):
            obj = reduce(lambda subitem, i: subitem.children[i] if isinstance(i, (int, long)) else subitem.child(i), treeindex[:-1], obj)
            treeindex = treeindex[-1]
        return treeindex, obj

    def __getitem__(self, treeindex):
        """Extract an item (subelement or attribute) at a given
        tree-index.

        In production code, use xmlobj.children[i] or
        xmlobj.attrib["something"] instead of xmlobj[i] or
        xmlobj["something"].
        """

        try:
            return self.attrib[treeindex]
        except KeyError:
            pass

        treeindex, obj = self._treeindex_descend(self, treeindex)

        if isinstance(treeindex, basestring):
            try:
                return obj.attrib[treeindex]
            except KeyError:
                return obj.child(treeindex)
        elif isinstance(treeindex, (int, long, slice)):
            return obj.children[treeindex]
        else:
            return obj.child(treeindex)

    def __setitem__(self, treeindex, value):
        """Set an item (subelement or attribute) at a given
        tree-index.

        In production code, use xmlobj.children[i] or
        xmlobj.attrib["something"] instead of xmlobj[i] or
        xmlobj["something"].
        """

        treeindex, obj = self._treeindex_descend(self, treeindex)

        if isinstance(treeindex, basestring):
            obj.attrib[treeindex] = value
        elif isinstance(treeindex, (int, long, slice)):
            obj.children[treeindex] = value
        else:
            obj.children[obj.index(treeindex)[0]] = value

    def __delitem__(self, treeindex):
        """Remove an item (subelement or attribute) at a given
        tree-index.

        In production code, use xmlobj.children[i] or
        xmlobj.attrib["something"] instead of xmlobj[i] or
        xmlobj["something"].
        """

        treeindex, obj = self._treeindex_descend(self, treeindex)

        if isinstance(treeindex, basestring):
            try:
                del obj.attrib[treeindex]
            except KeyError:
                del obj.children[obj.index(treeindex)]
        elif isinstance(treeindex, (int, long, slice)):
            del obj.children[treeindex]
        else:
            del obj.children[obj.index(treeindex)]

    def insert_before(self, treeindex, item):
        """Insert a new item before a given tree-index.
        
        In production code, use xmlobj.children.insert(i, item)
        instead of xmlobj.insert_before(i, item).
        """

        allbutlast = treeindex[:-1]
        last = treeindex[-1]

        if len(allbutlast) == 0:
            if isinstance(last, basestring):
                self.attrib[last] = item
            elif isinstance(last, (int, long)):
                self.children.insert(last, item)
            else:
                raise IndexError("treeindex must be [#, ..., #] or [#, ..., #, \"attrib\"]")
        else:
            if isinstance(last, basestring):
                self[allbutlast].attrib[last] = item
            elif isinstance(last, (int, long)):
                self[allbutlast].children.insert(last, item)
            else:
                raise IndexError("treeindex must be [#, ..., #] or [#, ..., #, \"attrib\"]")

    def insert_after(self, treeindex, item):
        """Insert a new item after a given tree-index."""

        allbutlast = treeindex[:-1]
        last = treeindex[-1]

        if len(allbutlast) == 0:
            if isinstance(last, basestring):
                self.attrib[last] = item
            elif isinstance(last, (int, long)):
                last += 1
                if last == len(self.children):
                    self.children.append(item)
                else:
                    self.children.insert(last, item)
            else:
                raise IndexError("treeindex must be [#, ..., #] or [#, ..., #, \"attrib\"]")
        else:
            if isinstance(last, basestring):
                self[allbutlast].attrib[last] = item
            elif isinstance(last, (int, long)):
                last += 1
                if last == len(self[allbutlast].children):
                    self[allbutlast].children.append(item)
                else:
                    self[allbutlast].children.insert(last, item)
            else:
                raise IndexError("treeindex must be [#, ..., #] or [#, ..., #, \"attrib\"]")

    ### recursively searching the tree
    class _DepthIterator(object):
        def __init__(self, obj, treeindex, prune, maxdepth, attrib, special):
            self.current = obj
            self.treeindex = treeindex
            self.shown = False
            self.prune = prune
            self.maxdepth = maxdepth
            self.attrib = attrib
            self.special = special

        def __iter__(self):
            return self

        def _children_iterators(self):
            if hasattr(self.current, "children") and len(self.current.children):
                def childiterator(s, i):
                    return self.__class__(s, self.treeindex + (i,), self.prune, self.maxdepth, self.attrib, self.special)
                return chain_from_iterable(map(childiterator, self.current.children, itertools.count()))
            return []

        def _attrib_iterators(self):
            if hasattr(self.current, "attrib") and len(self.current.attrib):
                items = self.current.attrib.items()
                items.sort()
                def attribiterator(item):
                    k, s = item
                    return self.__class__(s, self.treeindex + (k,), self.prune, self.maxdepth, self.attrib, self.special)
                return chain_from_iterable(map(attribiterator, items))
            return []

        def next(self):
            return self.__next__()

        def __next__(self):
            if self.special or not isinstance(self.current, XMLSpecial):
                if not self.shown:
                    self.shown = True
                    if self.treeindex != ():
                        return self.treeindex, self.current

            if self.maxdepth is not None and len(self.treeindex) >= self.maxdepth:
                raise StopIteration

            if not hasattr(self, "iterators"):
                if self.prune is None or not self.prune(self.current):
                    if self.attrib:
                        attribiterators = self._attrib_iterators() or []
                    else:
                        attribiterators = []

                    childiterators = self._children_iterators() or []

                else:
                    attribiterators = []
                    childiterators = []

                self.iterators = itertools.chain(attribiterators, childiterators)

            try:
                return self.iterators.__next__()
            except AttributeError:
                return self.iterators.next()

    ################ end nested class

    def walk(self, prune=None, maxdepth=None, attrib=False, special=False):
        """Recursively walk through an XML tree, like this::

            for treeindex, element in xmlobj.walk():
                do something with treeindex, element

        This does not include the top-level XML object (which does not
        have a tree-index).

        The 'prune' function prunes branches from consideration.
        Everything *below* the matching branch is skipped.

        Arguments::

            maxdepth (None or int): depth to traverse

            attrib (bool): if True, include attributes ('element'
                would sometimes be a string in the above example)

            special (bool): if True, include XMLSpecial elements such
                as XMLText and XMLComment
        """

        return self._DepthIterator(self, (), prune, maxdepth, attrib, special)

    def tree(self, prune=None, maxdepth=None, attrib=False, special=False, index_width=20, showtop=True):
        """Create a string representation of the XML tree, like this::

            print xmlobj.tree()

        The 'prune' function prunes branches from consideration.
        Everything *below* the matching branch is skipped.

        Arguments::

            maxdepth (None or int): depth to traverse

            attrib (bool): if True, include attributes

            special (bool): if True, include XMLSpecial elements

            index_width (int): number of characters wide for the index
                column

            showtop (bool): if True, show the top-level object
                (tree-index is None)
        """

        if showtop:
            output = [("%s %s" % (("%%-%ds" % index_width) % "index", repr(self)))]
        else:
            output = []

        for treeindex, element in self.walk(prune, maxdepth, attrib, special):
            output.append(("%s %s%s" % (("%%-%ds" % index_width) % repr(treeindex), ". . " * len(treeindex), repr(element))))
        return os.linesep.join(output)

    def descendant(self, test=None, which=0, maxdepth=None, exception=True):
        """Return the first descendant for whom 'test' is True.

            * If 'test' is None, match anything.
            * If 'test' is a class (subclass of XML), match that class.
            * If 'test' is a function, evaluate it to see if it
                returns True
            * If 'test' is a string, match the tag name.

        Other arguments::

            which (int): if non-zero, return the ith match rather than
                the first

            maxdepth (None or int): maximum depth for search; default
                is None (fully recursive)

            exception (bool): if True, raise an exception if the
                object is not found
        """

        for i, elem in self.walk(None, maxdepth, attrib=False, special=False):
            if test is None:
                if which == 0:
                    return elem
                else:
                    which -= 1

            elif inspect.isclass(test):
                if isinstance(elem, test):
                    if which == 0:
                        return elem
                    else:
                        which -= 1

            elif callable(test):
                try:
                    if test(elem):
                        if which == 0:
                            return elem
                        else:
                            which -= 1
                except:
                    pass

            elif isinstance(test, basestring):
                if elem.tag == test:
                    if which == 0:
                        return elem
                    else:
                        which -= 1

            else:
                raise TypeError("The 'test' of xml.descendant(test) must be a boolean, a class, a boolean-returning function, or a tag name string (XML tree of %s)" % self)

        if exception:
            raise ValueError("%s was not found in XML tree of %s (which=%d, maxdepth=%s)" % (repr(test), self, which, repr(maxdepth)))
        else:
            return None

    def index(self, test=None, which=0, maxdepth=1, attrib=False, exception=True):
        """Return the index of the first child for whom 'test' is True.

            * If 'test' is None, match anything.
            * If 'test' is a class (subclass of XML), match that class.
            * If 'test' is a function, evaluate it to see if it
                returns True
            * If 'test' is a string, match the tag name.

        Other arguments::

            which (int): if non-zero, return the ith match rather than
                the first

            maxdepth (None or int): maximum depth for search; default
                is 1 (this level only)

            attrib (bool): if True, include attributes in the search

            exception (bool): if True, raise an exception if the
                object is not found
        """

        testIsNone = testIsClass = testIsCallable = testIsString = False
        if test is None:
            testIsNone = True
        elif inspect.isclass(test):
            testIsClass = True
        elif callable(test):
            testIsCallable = True
        elif isinstance(test, basestring):
            testIsString = True
        else:
            raise TypeError("The 'test' of xml.index(test) must be a boolean, a class, a boolean-returning function, or a tag name string (XML tree of %s)" % self)

        for i, elem in self.walk(None, maxdepth, attrib, special=False):
            if testIsNone:
                if which == 0:
                    return i
                else:
                    which -= 1

            elif testIsClass:
                if isinstance(elem, test):
                    if which == 0:
                        return i
                    else:
                        which -= 1
            
            elif testIsCallable:
                try:
                    if test(elem):
                        if which == 0:
                            return i
                        else:
                            which -= 1
                except:
                    pass

            else:
                if elem.tag == test:
                    if which == 0:
                        return i
                    else:
                        which -= 1

        if exception:
            raise ValueError("%s was not found in XML tree of %s (which=%d, maxdepth=%s, attrib=%s)" % (repr(test), self, which, repr(maxdepth), repr(attrib)))
        else:
            return None

    def matches(self, test=None, maxdepth=1, attrib=False):
        """Return a list of children for whom 'test' is True.

            * If 'test' is None, match anything.
            * If 'test' is a class (subclass of XML), match that class.
            * If 'test' is a function, evaluate it to see if it
                returns True
            * If 'test' is a string, match the tag name.

        Other arguments::

            which (int): if non-zero, return the ith match rather than
                the first

            maxdepth (None or int): maximum depth for search; default
                is 1 (this level only)

            attrib (bool): if True, include attributes in the search
        """
        walkIterator = self.walk(None, maxdepth, attrib, special=False)

        if test is None:
            output = [elem for i, elem in walkIterator]

        elif inspect.isclass(test):
            output = [elem for i, elem in walkIterator if isinstance(elem, test)]

        elif callable(test):
            def tryTest(elem):
                try:
                    return test(elem)
                except:
                    return False
            output = [elem for i, elem in walkIterator if tryTest(elem)]

        elif isinstance(test, basestring):
            output = [elem for i, elem in walkIterator if elem.tag == test]

        else:
            raise TypeError("The 'test' of xml.matches(test) must be a boolean, a class, a boolean-returning function, or a tag name string (XML tree of %s)" % self)

        return output

    # even though this is a special case of descendant, it has been
    # implemented here for speed: xml.child() is by far the most
    # common use-case and a simple loop over self.children must be
    # faster than a general self.walk() with maxdepth=1
    def child(self, test=None, which=0, exception=True):
        """Return the first child for whom 'test' is True.

            * If 'test' is None, match anything.
            * If 'test' is a class (subclass of XML), match that class.
            * If 'test' is a function, evaluate it to see if it
                returns True
            * If 'test' is a string, match the tag name.

        Other arguments::

            which (int): if non-zero, return the ith match rather than
                the first

            exception (bool): if True, raise an exception if the
                object is not found

        This is like descendant with maxdepth=1, but faster.
        """

        for elem in self:
            if test is None:
                if which == 0:
                    return elem
                else:
                    which -= 1

            elif inspect.isclass(test):
                if isinstance(elem, test):
                    if which == 0:
                        return elem
                    else:
                        which -= 1

            elif callable(test):
                try:
                    if test(elem):
                        if which == 0:
                            return elem
                        else:
                            which -= 1
                except:
                    pass

            elif isinstance(test, basestring):
                if elem.tag == test:
                    if which == 0:
                        return elem
                    else:
                        which -= 1

            else:
                raise TypeError("The 'test' of xml.child(test) must be a boolean, a class, a boolean-returning function, or a tag name string (XML tree of %s)" % self)

        if exception:
            raise ValueError("%s was not found in XML tree of %s (which=%d, maxdepth=1)" % (repr(test), self, which))
        else:
            return None

    def exists(self, test, which=0, maxdepth=1):
        warnings.warn("Note to self: remember to change all xml.exists() to xml.contains()", DeprecationWarning)
        return self.contains(test, which, maxdepth)

    def contains(self, test, which=0, maxdepth=1):
        """Return True if 'test' is True for some child.

            * If 'test' is None, match anything.
            * If 'test' is a class (subclass of XML), match that class.
            * If 'test' is a function, evaluate it to see if it
                returns True
            * If 'test' is a string, match the tag name.

        Other arguments::

            which (int): if non-zero, return the ith match rather than
                the first

            maxdepth (None or int): maximum depth for search; default
                is 1 (this level only)

        This is like descendant with maxdepth=1, but faster.
        """

        if maxdepth == 1:
            return self.child(test, which, exception=False) is not None
        else:
            return self.descendant(test, which, maxdepth, exception=False) is not None

    def content(self):
        """Returns content of the element (any elements or text between the tags)."""
        return "".join([i.xml(indent="", linesep="") for i in self.children if not isinstance(i, XMLComment)])
        
    def textContent(self):
        """Returns a concatenation of just the text elements."""
        return os.linesep.join(sum([child.text for child in self.children if isinstance(child, XMLText)], []))

    # note that this is a loop over *non-special* children; otherwise just loop over self.children
    def __iter__(self):
        return (x for x in self.children if isinstance(x, XML) and not isinstance(x, XMLSpecial))

    ### how to present XML objects on the commandline (used in tree)
    def __repr__(self):
        attrib = [""]
        keys = self.attrib.keys()
        keys.sort()
        for key in keys:
            if isinstance(self.attrib[key], basestring):
                attrib.append("%s=\"%s\"" % (key, self.attrib[key]))
            else:
                attrib.append("%s=%s" % (key, str(self.attrib[key])))
        attrib = " ".join(attrib)

        quote = "\"" if self.__class__ is XML else ""

        if hasattr(self, "value"):
            if isinstance(self.value, basestring):
                value = "\"%s\"" % self.value
            else:
                value = repr(self.value)
            return "<%s%s%s%s %s at 0x%02x>" % (quote, self.tag, quote, attrib, value, id(self))

        if len(self.children) == 1:
            return "<%s%s%s%s (1 child) at 0x%02x>" % (quote, self.tag, quote, attrib, id(self))
        else:
            return "<%s%s%s%s (%d children) at 0x%02x>" % (quote, self.tag, quote, attrib, len(self.children), id(self))

    ### convert to text
    def _argToStr(self, value):
        # XML does not have capitalized True/False
        if value is True: return "true"
        if value is False: return "false"
        return str(value)

    def _xml(self, indent="    ", linesep=os.linesep, spaces="", fileobj=None):
        attrib = " ".join([""] + ["%s=%s" % (name, quoteattr(self._argToStr(value))) for name, value in self.attrib.items()])

        # if any of the text was converted into numerical items, convert them back now
        children = self.children
        if hasattr(self, "value"):
            try:
                iter(self.value)
                iterable = True
            except TypeError:
                iterable = False

            if iterable and not isinstance(self.value, basestring):
                children = [XMLText(" ".join(map(str, self.value)))] + children
            else:
                children = [XMLText(str(self.value))] + children

        if len(children) == 0:
            line = "%s<%s%s />" % (spaces, self.tag, attrib)
            if fileobj is None:
                return [line]
            else:
                fileobj.write(line)
                if linesep is not None: fileobj.write(linesep)
                return

        elif len(children) == 1 and isinstance(children[0], XMLText):
            if fileobj is None:
                return ["%s<%s%s>%s</%s>" % (spaces, self.tag, attrib, linesep.join(children[0]._xml(indent, linesep, spaces)), self.tag)]
            else:
                fileobj.write("%s<%s%s>" % (spaces, self.tag, attrib))
                children[0]._xml(indent, linesep, spaces, fileobj)
                fileobj.write("</%s>" % self.tag)
                if linesep is not None: fileobj.write(linesep)
                return

        else:
            if fileobj is None:
                lines = ["%s<%s%s>" % (spaces, self.tag, attrib)]
                for child in children:
                    lines.extend(child._xml(indent, linesep, indent + spaces))
                lines.append("%s</%s>" % (spaces, self.tag))
                return lines

            else:
                fileobj.write("%s<%s%s>" % (spaces, self.tag, attrib))
                if linesep is not None: fileobj.write(linesep)
                for child in children:
                    child._xml(indent, linesep, indent + spaces, fileobj)
                fileobj.write("%s</%s>" % (spaces, self.tag))
                if linesep is not None: fileobj.write(linesep)
                return

    def xml(self, indent="    ", linesep=os.linesep):
        """Convert an XML object to an XML string.

        Arguments::

            indent (str): indentation string (usually 4 spaces)

            linesep (char): line separation character (can be "")
        """

        return linesep.join(self._xml(indent, linesep, ""))

    def write(self, fileName, indent="    ", linesep=os.linesep, encoding="utf-8"):
        """Write an XML object to a file.

        Arguments::

            fileName (str): output file name

            indent (str): indentation string (usually 4 spaces)

            linesep (char): line separation character (can be "")

            encoding (str, None): encoding to use when writing the
                file (if None, only do unicode if needed)
        """

        if linesep == "":
            linesep = None

        if encoding is None:
            encoding = "ascii"

        f = codecs.open(fileName, "w", encoding=encoding)
        self._xml(indent, linesep, "", f)
        f.close()

    ### pickleability and value-based equality
    def __getstate__(self):
        state = {}
        if hasattr(self, "tag"): state["tag"] = self.tag
        if hasattr(self, "attrib"): state["attrib"] = self.attrib
        if hasattr(self, "children"): state["children"] = self.children
        if hasattr(self, "text"): state["text"] = self.text
        if hasattr(self, "value"): state["value"] = self.value
        return state

    def __setstate__(self, state):
        if "tag" in state: self.tag = state["tag"]
        if "attrib" in state: self.attrib = state["attrib"]
        if "children" in state: self.children = state["children"]
        if "text" in state: self.text = state["text"]
        if "value" in state: self.value = state["value"]

    def __eq__(self, other):
        if id(self) == id(other): return True
        if self.__class__ != other.__class__: return False
        if getattr(self, "tag", None) != getattr(other, "tag", None): return False
        if getattr(self, "attrib", None) != getattr(other, "attrib", None): return False
        if getattr(self, "children", None) != getattr(other, "children", None): return False
        if getattr(self, "text", None) != getattr(other, "text", None): return False
        if getattr(self, "value", None) != getattr(other, "value", None): return False
        return True

    def __ne__(self, other):
        return not (self == other)

    def __hash__(self):
        return hash("XML_%s_%s" % (self.tag, str(id(self))))

    def __deepcopy__(self, memo={}):
        # only copy the XML parts, not anything else that might be attached
        output = self.__class__.__new__(self.__class__)
        if hasattr(self, "tag"):
            output.tag = copy.deepcopy(self.tag, memo)
        if hasattr(self, "attrib"):
            output.attrib = copy.deepcopy(self.attrib, memo)
        if hasattr(self, "children"):
            output.children = copy.deepcopy(self.children, memo)
        if hasattr(self, "text"):
            output.text = copy.deepcopy(self.text, memo)
        if hasattr(self, "value"):
            output.value = copy.deepcopy(self.value, memo)
        memo[id(self)] = output
        return output

    def copy(self):
        """Deeply copy the XML object."""

        return copy.deepcopy(self)

    def validate(self, recurse=True, exception=False, index=None):
        """Validate XML against its XSD.

        The XML base class does not have an XSD.

        Arguments::

            recurse (bool): if False, validate only this level; if
                True, also validate subelements

            exception (bool): if True, raise an XMLValidationError on
                validation failures

            index (only used by recursion; set to None)
        """

        if index is None: index = []
        if self.xsd is None:
            return None

        if exception:
            if recurse:
                for i, child in enumerate(self.children):
                    index.append(i)
                    child.validate(recurse, True, index)
                    index.pop()
            self.xsd.validate(self)
            return None

        try:
            if recurse:
                for i, child in enumerate(self.children):
                    index.append(i)
                    child.validate(recurse, True, index)
                    index.pop()
            self.xsd.validate(self)
            return None

        except XMLValidationError as err:
            return str(err), index

    def fileAndLine(self, defaultFileName="<XML>"):
        """Return the file name and line numbers from whence this XML comes."""

        if hasattr(self, "lineStart") and hasattr(self, "lineEnd"):
            return " (%s:%d-%d)" % (getattr(self, "fileName", defaultFileName), self.lineStart, self.lineEnd)
        else:
            return ""

### special XML subclasses

class XMLSpecial(XML):
    """Base class for non-element XML objects: text, pre-processing
    instructions (e.g. <?php ?>), comments, and CDATA."""

    first_line = re.compile(r"^([^\n]*)\n")

    def __init__(self, text=None, forbidden=None):
        if text is None:
            self.text = []
        else:
            if forbidden is not None and text.find(forbidden) != -1:
                raise ValueError("%s must not include '%s'" % (self.__class__.__name__, forbidden))
            self.text = [text]

    def __str__(self):
        if len(self.text) > 1:
            return os.linesep.join(self.text)
        elif len(self.text) == 1:
            return self.text[0]
        else:
            return ""

    def _repr_text(self):
        text = str(self)
        if len(text) > 30:
            text = "%s..." % text[:27]
        m = re.match(self.first_line, text)
        if m is not None:
            text = "%s..." % m.group(1)
        return text

    def __repr__(self):
        return "<XMLSpecial \"%s\" at 0x%02x>" % (self._repr_text(), id(self))

    def validate(self, recurse=True, exception=False, index=None):
        return None

class XMLText(XMLSpecial):
    """Represent a string within an XML structure."""

    def __repr__(self):
        return "<XMLText \"%s\" at 0x%02x>" % (self._repr_text(), id(self))

    def _xml(self, indent="    ", linesep=os.linesep, spaces="", fileobj=None):
        if fileobj is None:
            return [escape(str(self))]
        else:
            fileobj.write(escape(str(self)))

        # text = str(self)
        # for i in xrange(32):
        #     if i not in (10, 13):
        #         text = text.replace(chr(i), "%" + str(i))
        # text = text.replace(chr(127), "%" + str(127))
        # for old, newnew in (("&", "&amp;"), ("'", "&apos;"), ('"', "&quot;"), (">", "&gt;"), ("<", "&lt;")):
        #     text = text.replace(old, newnew)
        # return [text]

class XMLInstruction(XMLSpecial):
    """Represent a pre-processing instruction (e.g. <?php ?>) within
    an XML structure."""

    def __init__(self, tag, text=None):
        self.tag = tag
        XMLSpecial.__init__(self, text, "?>")

    def __repr__(self):
        return "<XMLInstruction %s \"%s\" at 0x%02x>" % (self.tag, self._repr_text(), id(self))

    def _xml(self, indent="    ", linesep=os.linesep, spaces="", fileobj=None):
        if fileobj is None:
            return ["%s<?%s %s ?>" % (spaces, self.tag, str(self))]
        else:
            fileobj.write("%s<?%s %s ?>" % (spaces, self.tag, str(self)))
            if linesep is not None:
                fileobj.write(linesep)

class XMLComment(XMLSpecial):
    """Represent a comment within an XML structure."""

    def __init__(self, text=None):
        XMLSpecial.__init__(self, text, "--")

    def __repr__(self):
        return "<XMLComment \"%s\" at 0x%02x>" % (self._repr_text(), id(self))

    def _xml(self, indent="    ", linesep=os.linesep, spaces="", fileobj=None):
        if fileobj is None:
            return ["%s<!-- %s -->" % (spaces, str(self))]
        else:
            fileobj.write("%s<!-- %s -->" % (spaces, str(self)))
            if linesep is not None:
                fileobj.write(linesep)

class XMLCDATA(XMLSpecial):
    """Represent a CDATA object within an XML structure."""

    def __init__(self, text=None):
        XMLSpecial.__init__(self, text, "]]>")

    def __repr__(self):
        return "<XMLCDATA \"%s\" at 0x%02x>" % (self._repr_text(), id(self))

    def _xml(self, indent="    ", linesep=os.linesep, spaces="", fileobj=None):
        if fileobj is None:
            return ["%s<![CDATA[%s]]>" % (spaces, str(self))]
        else:
            fileobj.write("%s<![CDATA[%s]]>" % (spaces, str(self)))
            if linesep is not None:
                fileobj.write(linesep)

### validating XML against an XSD

def validateBoolean(value):
    """Apply XML rules for booleans."""

    if value in (True, "true", "1", 1):
        return True
    elif value in (False, "false", "0", 0):
        return False
    else:
        raise XMLValidationError("\"%s\" is not a boolean (only \"true\", \"false\", \"1\", or \"0\")" % value)

class XSDBuiltinTypeCheck:
    """Base class for XSD type checking objects."""

    typeMap = {
        None: "self.validateString",
        "xs:anyType": "self.validateString",
        "xs:string": "self.validateString",
        "xs:NMTOKEN": "self.validateNMTOKEN",
        "xs:anyURI": "self.validateURI",
        "xs:integer": "self.validateInteger",
        "xs:int": "self.validateInteger",
        "xs:nonNegativeInteger": "self.validateNonNegativeInteger",
        "xs:float": "self.validateFloat",
        "xs:double": "self.validateFloat",
        "xs:decimal": "self.validateFloat",
        "xs:boolean": "self.validateBoolean",
        }

    regexpNMTOKEN = re.compile("^[A-Za-z0-9\.:-]*$")
    regexpURI = re.compile("^(([^:/?#]+):)?(//([^/?#]*))?([^?#]*)(\?([^#]*))?(#(.*))?$")

    def validationFunc(self, dataType):
        if dataType in self.typeMap:
            return eval(self.typeMap[dataType])
        else:
            raise XSDDefinitionError("Type \"%s\" is either an XSD built-in type that hasn't been implemented or a misspelling" % dataType)

    def validateString(self, value):
        return str(value)

    def validateNMTOKEN(self, value):
        if re.match(self.regexpNMTOKEN, value) is None:
            raise XMLValidationError("Attribute \"%s\": \"%s\" is not an NMTOKEN (characters, digits, period, colon, hyphen)" % (self.name, value))
        return value

    def validateURI(self, value):
        if re.match(self.regexpURI, value) is None:
            raise XMLValidationError("Attribute \"%s\": \"%s\" is not a URI" % (self.name, value))
        return value
            
    def validateInteger(self, value):
        try:
            return int(value)
        except (ValueError, TypeError):
            raise XMLValidationError("Attribute \"%s\": \"%s\" is not an integer" % (self.name, value))

    def validateNonNegativeInteger(self, value):
        try:
            tmp = int(value)
            if tmp < 0.:
                raise ValueError
            return tmp
        except (ValueError, TypeError):
            raise XMLValidationError("Attribute \"%s\": \"%s\" is not a non-negative integer" % (self.name, value))

    def validateFloat(self, value):
        try:
            return float(value)
        except (ValueError, TypeError):
            raise XMLValidationError("Attribute \"%s\": \"%s\" is not a float" % (self.name, value))

    def validateBoolean(self, value):
        try:
            return validateBoolean(value)
        except XMLValidationError:
            raise XMLValidationError("Attribute \"%s\": \"%s\" is not a boolean (only \"true\", \"false\", \"1\", or \"0\")" % (self.name, value))
            
class XSDAttribute(XSDBuiltinTypeCheck):
    """Represent an XSD attribute."""

    def __init__(self, xsd, xsdType):
        self.name = xsd["name"]
        self.required = (xsd.attrib.get("use", "optional") == "required")
        self.default = xsd.attrib.get("default", None)

        self.type = xsd.attrib.get("type", None)
        if self.type is None and len(xsd.children) > 0:
            self.type = xsd[0]

        elif self.type in self.typeMap:
            self.validateType = self.validationFunc(self.type)

        else:
            if self.type not in xsdType:
                raise XSDDefinitionError("Type \"%s\" is missing from the XSD" % self.type)
            self.type = xsdType[self.type]
            
        if isinstance(self.type, XML) and self.type.tag == "xs:simpleType" and len(self.type.children) == 1 and self.type[0].tag == "xs:restriction":
            self.enumeration = []
            for child in self.type[0].children:
                if child.tag == "xs:enumeration":
                    self.enumeration.append(child["value"])
                else:
                    raise NotImplementedError
            
            if self.type[0, "base"] in ("xs:string", "xs:anyType"):
                self.validateBase = self.validateString

            elif self.type[0, "base"] == "xs:NMTOKEN":
                self.validateBase = self.validateNMTOKEN

            elif self.type[0, "base"] in ("xs:integer", "xs:int"):
                self.validateBase = self.validateInteger

            elif self.type[0, "base"] == "xs:nonNegativeInteger":
                self.validateBase = self.validateNonNegativeInteger

            elif self.type[0, "base"] in ("xs:float", "xs:double", "xs:decimal"):
                self.validateBase = self.validateFloat

            self.validateType = self.validateEnumeration
        
    def validateEnumeration(self, value):
        value = self.validateBase(value)

        if len(self.enumeration) == 0:
            return value

        if value in self.enumeration:
            return value
        else:
            raise XMLValidationError("Attribute \"%s\": \"%s\" is not in %s" % (self.name, value, repr(self.enumeration)))

class XSDMinMax(object):
    """Base class for XSD objects with a minOccurs and a maxOccurs."""

    def __init__(self, xsd):
        XSDMinMax.minmax(self, xsd)

    def initchildren(self, xsd, xsdGroup, classMap):
        self.contents = []
        self.name = []

        for child in xsd.children:
            if child.tag == "xs:element":
                self.contents.append(XSDElementRef(child, classMap))
                self.name.append(child["ref"])

            elif child.tag == "xs:sequence":
                self.contents.append(XSDSequence(child, xsdGroup, classMap))
                self.name.append("<xs:sequence>")

            elif child.tag == "xs:choice":
                self.contents.append(XSDChoice(child, xsdGroup, classMap))
                self.name.append("<xs:choice>")

            elif child.tag == "xs:all":
                self.contents.append(XSDAll(child, xsdGroup, classMap))
                self.name.append("<xs:all>")

            elif child.tag == "xs:group":
                obj = xsdGroup[child["ref"]][0]

                if obj.tag == "xs:sequence":
                    s = XSDSequence(obj, xsdGroup, classMap)

                elif obj.tag == "xs:choice":
                    s = XSDChoice(obj, xsdGroup, classMap)

                elif obj.tag == "xs:all":
                    s = XSDAll(obj, xsdGroup, classMap)

                else: raise NotImplementedError

                s.minmax(child) # get the minOccurs and maxOccurs from the reference
                self.contents.append(s)
                self.name.append("<group %s>" % child["ref"])

            else:
                raise NotImplementedError

    def minmax(self, xsd):
        try:
            self.minOccurs = int(xsd.attrib.get("minOccurs", 1))
        except ValueError:
            self.minOccurs = None
        try:
            self.maxOccurs = int(xsd.attrib.get("maxOccurs", 1))
        except ValueError:
            self.maxOccurs = None

class XSDElementRef(XSDMinMax):
    """Represent an XSD element reference."""

    def __init__(self, xsd, classMap):
        XSDMinMax.__init__(self, xsd)
        self.name = xsd["ref"]
        self.classMap = classMap

    def somethingNeeded(self):
        return (self.minOccurs > 0)

    def incrementIndex(self, xmlChildren, index):
        matches = 0
        while index < len(xmlChildren) and (getattr(xmlChildren[index], "tag", None) == self.name or isinstance(xmlChildren[index], self.classMap[self.name])):
            matches += 1
            index += 1
            if self.maxOccurs is not None and matches == self.maxOccurs:
                break

        if self.minOccurs is not None:
            if matches < self.minOccurs:
                if self.minOccurs == 1:
                    raise XMLValidationError("Element \"%s\" not found" % self.name)
                else:
                    raise XMLValidationError("Too few \"%s\" (%d observed, minimum is %d)" % (self.name, matches, self.minOccurs))

        return index

class XSDSequence(XSDMinMax):
    """Represent an XSD sequence."""

    def __init__(self, xsd, xsdGroup, classMap):
        XSDMinMax.__init__(self, xsd)
        self.initchildren(xsd, xsdGroup, classMap)
        self.name = ", ".join(self.name)

    def somethingNeeded(self):
        if hasattr(self, "_somethingNeeded"):
            return self._somethingNeeded

        if self.minOccurs == 0:
            self._somethingNeeded = False
            return False

        for content in self.contents:
            if content.somethingNeeded():
                self._somethingNeeded = True
                return True
        self._somethingNeeded = False
        return False

    def incrementIndex(self, xmlChildren, index):
        matches = 0
        complaints = False

        while index < len(xmlChildren):
            newIndex = index
            try:
                for content in self.contents:
                    newIndex = content.incrementIndex(xmlChildren, newIndex)

            except XMLValidationError:
                newIndex = index
                complaints = True

            if newIndex > index:
                matches += 1
                index = newIndex
                if self.maxOccurs is not None and matches == self.maxOccurs:
                    break

            else:
                break

        if self.minOccurs is not None:
            if matches < self.minOccurs and (complaints or self.somethingNeeded()):
                if self.minOccurs == 1:
                    raise XMLValidationError("Pattern of xs:sequence block \"%s\" not matched" % self.name)
                else:
                    raise XMLValidationError("Too few \"%s\" xs:sequences (%d observed, minimum is %d)" % (self.name, matches, self.minOccurs))

        return index

class XSDChoice(XSDMinMax):
    """Represent an XSD choice block."""

    def __init__(self, xsd, xsdGroup, classMap):
        XSDMinMax.__init__(self, xsd)
        self.initchildren(xsd, xsdGroup, classMap)
        self.name = " or ".join(self.name)

    def somethingNeeded(self):
        if hasattr(self, "_somethingNeeded"):
            return self._somethingNeeded

        if self.minOccurs == 0:
            self._somethingNeeded = False
            return False
        for content in self.contents:
            if not content.somethingNeeded():
                self._somethingNeeded = False
                return False
        self._somethingNeeded = True
        return True

    def incrementIndex(self, xmlChildren, index):
        matches = 0
        while index < len(xmlChildren):
            newIndex = index

            for content in self.contents:
                try:
                    newIndex = content.incrementIndex(xmlChildren, index)

                    if newIndex > index:
                        break
                except XMLValidationError:
                    pass

            if newIndex > index:
                matches += 1
                index = newIndex
                if self.maxOccurs is not None and matches == self.maxOccurs:
                    break

            else:
                break

        if self.minOccurs is not None:
            if matches < self.minOccurs and self.somethingNeeded():
                if self.minOccurs == 1:
                    raise XMLValidationError("Pattern of xs:choice block \"%s\" not matched" % self.name)
                else:
                    raise XMLValidationError("Too few \"%s\" xs:choice blocks (%d observed, minimum is %d)" % (self.name, matches, self.minOccurs))

        return index

class XSDAll(XSDMinMax):
    """Represent an XSD all block."""

    def __init__(self, xsd, xsdGroup, classMap):
        XSDMinMax.__init__(self, xsd)
        self.initchildren(xsd, xsdGroup, classMap)

        for content in self.contents:
            if content.minOccurs not in (0, 1):
                raise XSDDefinitionError("minOccurs of content in an <xs:all> block may not be \"%s\"" % str(content.minOccurs))
            if content.maxOccurs != 1:
                raise XSDDefinitionError("maxOccurs of content in an <xs:all> block may not be \"%s\"" % str(content.maxOccurs))

        if self.minOccurs not in (0, 1):
            raise XSDDefinitionError("xs:all minOccurs=\"%s\" hasn't been implemented" % self.minOccurs)

        if self.maxOccurs != 1:
            raise XSDDefinitionError("xs:all maxOccurs=\"%s\" hasn't been implemented" % self.maxOccurs)

        self.needs = [id(c) for c in self.contents if c.minOccurs == 1]
        self.needNames = " and ".join([c.name for c in self.contents if c.minOccurs == 1])

        self.name = " and ".join(self.name)

    def incrementIndex(self, xmlChildren, index):
        if self.minOccurs == 0: return index

        matches = dict([(id(content), 0) for content in self.contents])

        while index < len(xmlChildren):
            newIndex = index

            for content in self.contents:
                try:
                    newIndex = content.incrementIndex(xmlChildren, index)

                    if newIndex > index:
                        matches[id(content)] += 1
                        break

                except XMLValidationError:
                    pass

            if newIndex > index:
                index = newIndex
            else:
                missing = [need for need in self.needs if matches[need] == 0]
                if len(missing) > 0:
                    raise XMLValidationError("Unrecognized \"%s\" before required elements \"%s\" were found in xs:all block \"%s\"" % (getattr(xmlChildren[index], "tag"), self.needNames, self.name))
                break

        for content in self.contents:
            if matches[id(content)] > 1:
                raise XMLValidationError("Too many \"%s\" in xs:all block \"%s\"" % (content.name, self.name))

        return index

class XSDList(XSDBuiltinTypeCheck):
    """Represent an XSD text-based list."""

    findall = re.compile("(\"[^\"]+\"|[^\s]+)")

    def __init__(self, name, itemType):
        self.name = name
        self.theType = itemType
        self.validateType = self.validationFunc(itemType)

    def validateText(self, text):
        out = map(self.validateType, [x.lstrip("\"").rstrip("\"") for x in re.findall(self.findall, text)])
        if not isinstance(out, list): out = list(out)
        return out

class XSDValue(XSDBuiltinTypeCheck):
    """Represent an XSD text-based value."""

    def __init__(self, name, base):
        self.name = name
        self.theType = base
        self.validateType = self.validationFunc(base)

    def validateText(self, text):
        return self.validateType(text)

class XSDElement(object):
    """Represent an XSD element.

    Not all possible XSD configurations are implemented, only those
    that appear in PMML.
    """

    def __init__(self, xsd, baseClass):
        self.name = xsd["name"]
        self.attributes = {}
        self.attributeOrder = []
        self.contents = []
        self.justCheckNumber = False
        self.textExpected = None
        self.processContents = True
        
        if "type" in xsd.attrib and len(xsd.children) == 0:
            attrib = xsd.attrib
            xsd = XML(xsd.tag, baseClass.xsdType[xsd["type"]])
            xsd.attrib = attrib

        if len(xsd.children) == 0:
            pass

        elif len(xsd.children) == 1 and xsd[0].tag == "xs:complexType" and not (len(xsd[0].children) == 1 and xsd[0,0].tag in ("xs:simpleContent", "xs:complexContent")):
            if validateBoolean(xsd[0].attrib.get("mixed", "false")):
                self.textExpected = "any"

            for child in xsd[0].children:
                if child.tag == "xs:attribute":
                    self.attributes[child["name"]] = XSDAttribute(child, baseClass.xsdType)
                    self.attributeOrder.append(child["name"])
                    
                elif child.tag == "xs:sequence":
                    self.contents.append(XSDSequence(child, baseClass.xsdGroup, baseClass.classMap))

                elif child.tag == "xs:choice":
                    self.contents.append(XSDChoice(child, baseClass.xsdGroup, baseClass.classMap))

                elif child.tag == "xs:all":
                    self.contents.append(XSDAll(child, baseClass.xsdGroup, baseClass.classMap))

                elif child.tag == "xs:group":
                    obj = baseClass.xsdGroup[child["ref"]][0]

                    if obj.tag == "xs:sequence":
                        s = XSDSequence(obj, baseClass.xsdGroup, baseClass.classMap)

                    elif obj.tag == "xs:choice":
                        s = XSDChoice(obj, baseClass.xsdGroup, baseClass.classMap)

                    elif obj.tag == "xs:all":
                        s = XSDAll(obj, baseClass.xsdGroup, baseClass.classMap)

                    else: raise NotImplementedError

                    s.minmax(child) # get the minOccurs and maxOccurs from the reference
                    self.contents.append(s)

                else:
                    raise NotImplementedError(child.tag)

        elif len(xsd.children) == 1 and xsd[0].tag == "xs:complexType" and len(xsd[0].children) == 1 and xsd[0,0].tag == "xs:simpleContent" and len(xsd[0,0].children) == 1 and xsd[0,0,0].tag == "xs:extension":
            if validateBoolean(xsd[0].attrib.get("mixed", "false")):
                self.textExpected = "any"

            for child in xsd[0,0,0].children:
                if child.tag == "xs:attribute":
                    self.attributes[child["name"]] = XSDAttribute(child, baseClass.xsdType)
                    self.attributeOrder.append(child["name"])

            self.textExpected = XSDValue(self.name, xsd[0,0,0,"base"])

        elif len(xsd.children) == 1 and xsd[0].tag == "xs:simpleType" and len(xsd[0].children) == 1 and xsd[0,0].tag == "xs:list":
            self.textExpected = XSDList(self.name, xsd[0,0,"itemType"])

        elif len(xsd.children) == 1 and xsd[0].tag == "xs:complexType" and len(xsd[0].children) == 1 and xsd[0,0].tag == "xs:complexContent" and len(xsd[0,0].children) == 1 and xsd[0,0,0].tag == "xs:restriction" and xsd[0,0,0,"base"] == "xs:anyType" and len(xsd[0,0,0].children) > 0 and xsd[0,0,0,0].tag == "xs:sequence" and len(xsd[0,0,0,0].children) > 0:
            if validateBoolean(xsd[0].attrib.get("mixed", "false")):
                self.textExpected = "any"

            ## TODO: make xs:any a XSDAny object that ignores content

            self.processContents = (xsd[0,0,0,0,0].attrib.get("processContents", "strict") != "skip")

            for child in xsd[0,0,0].children:
                if child.tag == "xs:attribute":
                    self.attributes[child["name"]] = XSDAttribute(child, baseClass.xsdType)
                    self.attributeOrder.append(child["name"])

            self.justCheckNumber = XSDMinMax(xsd[0,0,0,0,0])
            
        else:
            raise NotImplementedError

    def validate(self, xml):
        # check attributes
        for observedName in xml.attrib.keys():
            if observedName not in self.attributes:
                raise XMLValidationError("Unexpected attribute \"%s\"" % observedName)

        for expectedName, xsdAttribute in self.attributes.items():
            if xsdAttribute.required and expectedName not in xml.attrib:
                raise XMLValidationError("Missing attribute \"%s\"" % expectedName)

            if expectedName in xml.attrib:
                xml.attrib[expectedName] = xsdAttribute.validateType(xml.attrib[expectedName])

        # split content into text and subelements
        xmlText = []
        xmlChildren = []
        xmlChildrenWithSpecial = []
        for child in xml.children:
            if isinstance(child, XMLText):
                xmlText.append(child)
            else:
                if isinstance(child, XMLSpecial):
                    xmlChildrenWithSpecial.append(child)
                else:
                    xmlChildrenWithSpecial.append(child)
                    xmlChildren.append(child)

        # check subelements
        index = 0
        if self.justCheckNumber is False:
            for content in self.contents:
                index = content.incrementIndex(xmlChildren, index)

            if index != len(xmlChildren):
                cardinal = index + 1
                if cardinal == 1: cardinal = "1st"
                elif cardinal == 2: cardinal = "2nd"
                elif cardinal == 3: cardinal = "3rd"
                else: cardinal = "%dth" % cardinal
                raise XMLValidationError("Too many subelements: %s (\"%s\") was unexpected" % (cardinal, getattr(xmlChildren[index], "tag", getattr(xmlChildren[index], "text", repr(xmlChildren[index])))))

        else:
            if self.justCheckNumber.minOccurs is not None:
                if len(xmlChildren) < self.justCheckNumber.minOccurs:
                    raise XMLValidationError("Too few subelements (%d observed, minimum is %d)" % (len(xmlChildren), self.justCheckNumber.minOccurs))

            if self.justCheckNumber.maxOccurs is not None:
                if len(xmlChildren) > self.justCheckNumber.maxOccurs:
                    raise XMLValidationError("Too many subelements (%d observed, maximum is %d)" % (len(xmlChildren), self.justCheckNumber.maxOccurs))

        # check text
        if self.textExpected is None:
            if len(xmlText) > 0:
                raise XMLValidationError("Text content \"%s\" unexpected" % str(xmlText[0]))

        elif self.textExpected is "any":
            pass

        else:
            # combine all text elements together and remove leading whitespace for text-check algorithm, then turn into numbers
            xml.value = self.textExpected.validateText("".join([str(x) for x in xmlText]).lstrip(string.whitespace).rstrip(string.whitespace))
            xml.children = xmlChildrenWithSpecial

        if hasattr(xml, "post_validate"):
            xml.post_validate()

### loading XML

def load_xsdType(stream):
    """Shortcut for loading an XSD type object."""
    return load(stream, dropSpecial=True)

def load_xsdGroup(stream):
    """Shortcut for loading an XSD group object."""
    return load(stream, dropSpecial=True)

def load_xsdElement(baseClass, stream):
    """Shortcut for loading an XSD element."""
    output = XSDElement(load(stream, dropSpecial=True), baseClass)
    output.__str__ = lambda: stream
    return output

def loadfile(fileName, base=None, validation=True, dropSpecial=False, lineNumbers=False):
    """Load a file with name fileName.

    Other arguments::

        base (None or class): base class of XSD schema to use for
            validation

        validation (bool): if False, don't validate (but still use
            'base' for class names, to possibly validate later)

        dropSpecial (bool): if True, exclude all special objects

        lineNumbers (bool): if True, insert line numbers in all loaded
            XML objects (more information for the user)

    """

    if fileName.startswith("http://") or fileName.startswith("https://"):
        try:
            connection = urllib.urlopen(fileName)
        except IOError as err:
            raise IOError("Unable to retrieve URL \"%s\": %s" % (fileName, str(err)))

        responseCode = connection.getcode()
        if responseCode == 200:
            return load(connection, base, validation, dropSpecial, lineNumbers)
        else:
            raise IOError("Unable to retrieve URL \"%s\": response code is %d" % (fileName, responseCode))

    else:
        return load(file(fileName), base, validation, dropSpecial, lineNumbers)

def load(stream, base=None, validation=True, dropSpecial=False, lineNumbers=False):
    """Load a stream as XML (see loadfile if you have a fileName).

    Arguments::

        base (None or class): base class of XSD schema to use for
            validation

        validation (bool): if False, don't validate (but still use
            'base' for class names, to possibly validate later)

        dropSpecial (bool): if True, exclude all special objects

        lineNumbers (bool): if True, insert line numbers in all loaded
            XML objects (more information for the user)
    """

    class ContentHandler(xml.sax.handler.ContentHandler):
        all_whitespace = re.compile(r"^\s*$")
        leading_or_trailing_space = re.compile("(^ | $)")
        trailing_space = re.compile(" $")

        def __init__(self, b):
            self.stack = []
            self.output = None
            self.CDATA = False
            self.base = b
            self.basestack = []

        def startElement(self, tag, attrib):
            if self.base is None or self.base.classMap is None:
                classObj = None
            else:
                classObj = self.base.classMap.get(tag, None)

            if classObj is None:
                s = XML(tag)
                s.attrib = dict([(str(k),v) for k, v in attrib.items()])
            else:
                s = classObj.__new__(classObj)
                s.tag = tag
                s.children = []
                s.attrib = dict([(str(k),v) for k, v in attrib.items()])

            if lineNumbers:
                s.lineStart = self._parser.getLineNumber()
                if self._fileName is not None:
                    s.fileName = self._fileName

            if len(self.stack) > 0:
                last = self.stack[-1]
                last.children.append(s)
            self.stack.append(s)

            if hasattr(s, "embeddedBase"):
                self.basestack.append(self.base)
                self.base = s.embeddedBase

        def characters(self, text):
            if not dropSpecial:
                if self.CDATA:
                    last = self.stack[-1]
                    last.text.append(text)

                elif self.all_whitespace.match(text) is None:
                    if len(self.stack) > 0:
                        last = self.stack[-1]
                        if len(last.children) == 0 or not isinstance(last.children[-1], XMLText):
                            s = XMLText()
                            last.children.append(s)
                            self.last_text_linenumber = self._parser.getLineNumber()
                        last = last.children[-1]

                        if self.last_text_linenumber == self._parser.getLineNumber() and len(last.text) > 0:
                            last.text[-1] += text
                        else:
                            last.text.append(text)
                        self.last_text_linenumber = self._parser.getLineNumber()

        def endElement(self, tag):
            if len(self.stack) > 0:
                last = self.stack[-1]
                for child in last.children:
                    if isinstance(child, XMLText):
                        child.text = [str(child)]

                if lineNumbers:
                    last.lineEnd = self._parser.getLineNumber()

                if validation:
                    try:
                        last.validate(recurse=False, exception=True)
                    except XMLValidationError as err:
                        stacktrace = "Below is a traceback to the line that caused the actual exception.\n" + "".join(traceback.format_tb(sys.exc_info()[2]))
                        if lineNumbers:
                            if last.tag != last.topTag:
                                raise XMLValidationError("%sXMLValidationError: %s in element <%s> on lines %d-%d%s" % (stacktrace, str(err), last.tag, last.lineStart, last.lineEnd, "" if self._fileName is None else " of file \"%s\"." % self._fileName))
                            else:
                                raise XMLValidationError("%sXMLValidationError: %s." % (stacktrace, str(err)))
                        
                        else:
                            if last.tag != last.topTag:
                                raise XMLValidationError("%sXMLValidationError: %s in element <%s> ending on line %d%s" % (stacktrace, str(err), last.tag, self._parser.getLineNumber(), "" if self._fileName is None else " of file \"%s\"." % self._fileName))
                            else:
                                raise XMLValidationError("%sXMLValidationError: %s." % (stacktrace, str(err)))

                if hasattr(last, "embeddedBase"):
                    self.base = self.basestack.pop()

            self.output = self.stack.pop()

        def processingInstruction(self, target, data):
            if not dropSpecial: 
                s = XMLInstruction(target, re.sub(self.trailing_space, "", data))
                if len(self.stack) > 0:
                    last = self.stack[-1]
                    last.children.append(s)
                if len(self.stack) != 0:
                    self.output = s

        def comment(self, comment):
            if not dropSpecial: 
                s = XMLComment(re.sub(self.leading_or_trailing_space, "", comment))
                if len(self.stack) > 0:
                    last = self.stack[-1]
                    last.children.append(s)
                if len(self.stack) != 0:
                    self.output = s

        def startCDATA(self):
            if not dropSpecial: 
                s = XMLCDATA()
                if len(self.stack) > 0:
                    last = self.stack[-1]
                    last.children.append(s)
                self.stack.append(s)
                self.CDATA = True

        def endCDATA(self):
            if not dropSpecial: 
                if len(self.stack) > 0:
                    last = self.stack[-1]
                if len(self.stack) != 0:
                    self.output = self.stack.pop()
                self.CDATA = False

        def startDTD(self, name, public_id, system_id):
            pass

        def endDTD(self):
            pass

        def startEntity(self, name):
            pass

        def endEntity(self, name):
            pass

    if isinstance(stream, bytes):
        stream = BytesIO(stream)
    elif isinstance(stream, basestring):
        stream = BytesIO(stream.encode("utf-8"))

    content_handler = ContentHandler(base)
    parser = xml.sax.make_parser()
    parser.setContentHandler(content_handler)
    try:
        content_handler._fileName = stream.name
    except AttributeError:
        content_handler._fileName = None
    content_handler._parser = parser

    parser.setProperty(xml.sax.handler.property_lexical_handler, content_handler)
    parser.setFeature(xml.sax.handler.feature_namespaces, 0)
    parser.setFeature(xml.sax.handler.feature_external_ges, 0)

    try:
        parser.parse(stream)
    except xml.sax._exceptions.SAXParseException as err:
        raise XMLError(err)

    if validation and base is not None and base.classMap is not None:
        if content_handler.output.tag not in base.classMap:
            raise XMLValidationError("Unrecognized tag \"%s\" at top of XML tree" % content_handler.output.tag)

    return content_handler.output
