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

"""This module defines as many lxml.etree classes and functions as are
needed to run Augusuts."""

try:
    import re
    import gzip

    import jarray
    import java.io.File as File
    import java.io.ByteArrayOutputStream as ByteArrayOutputStream
    import java.util.zip.Deflater as Deflater

    import javax.xml.parsers.DocumentBuilderFactory as DocumentBuilderFactory
    import javax.xml.namespace.NamespaceContext as NamespaceContext
    import javax.xml.XMLConstants as XMLConstants
    import javax.xml.xpath.XPathFactory as XPathFactory
    import javax.xml.xpath.XPathConstants as XPathConstants
    import javax.xml.xpath.XPathExpressionException as XPathExpressionException
    import javax.xml.transform.TransformerFactory as TransformerFactory
    import javax.xml.transform.dom.DOMSource as DOMSource
    import javax.xml.transform.stream.StreamResult as StreamResult

    import org.w3c.dom.Node as Node
    import org.w3c.dom.DOMException as DOMException

    documentFactory = DocumentBuilderFactory.newInstance()
    documentFactory.setNamespaceAware(True)
    documentBuilder = documentFactory.newDocumentBuilder()

    xpathFactory = XPathFactory.newInstance()

    transformerFactory = TransformerFactory.newInstance()
    identityTransformation = transformerFactory.newTransformer()

    class XPathEvalError(Exception): pass

    class _ElementStringResult(unicode):
        @classmethod
        def _create(cls, node):
            if node.getNodeType() == Node.ATTRIBUTE_NODE:
                self = _ElementStringResult(node.getValue())
                self.attrname = node.getName()
                self.is_attribute = True
                self.is_text = False
                self.is_tail = False

                owner = node.getOwnerElement()
                if owner is None:
                    self._parent = None
                else:
                    self._parent = _Element._create(owner)

            else:
                self = _ElementStringResult(node.getNodeValue())
                self.attrname = None
                self.is_attribute = False

                parent = node.getParentNode()
                if parent is None:
                    self._parent = None
                    self.is_text = False
                    self.is_tail = False

                elif parent.getFirstChild() == node:
                    self._parent = _Element._create(parent)
                    self.is_text = True
                    self.is_tail = False

                else:
                    parent = node.getPreviousSibling()
                    if parent is None:
                        self._parent = None
                        self.is_text = False
                        self.is_tail = False

                    else:
                        self._parent = _Element._create(parent)
                        self.is_text = False
                        self.is_tail = True

            return self

        def getparent(self):
            return self._parent

    class _Attrib(object):
        def __init__(self, element):
            self.element = element

        def __repr__(self):
            return repr(dict(self))

        def __contains__(self, key):
            return self.has_key(key)

        def __delitem__(self, key):
            try:
                node.getAttributes().removeNamedItem(key)
            except DOMException:
                raise KeyError(key)

        def __eq__(self, other):
            return dict(self) == dict(other)

        def __ge__(self, other):
            return dict(self) >= dict(other)

        def __getitem__(self, key):
            out = self.element.get(key)
            if out is None:
                raise KeyError(key)
            else:
                return out

        def __gt__(self, other):
            return dict(self) > dict(other)

        def __iter__(self):
            return self.iterkeys()

        def __le__(self, other):
            return dict(self) <= dict(other)

        def __len__(self):
            return len(self.keys())

        def __lt__(self):
            return dict(self) < dict(other)

        def __ne__(self):
            return dict(self) != dict(other)

        def __nonzero__(self):
            return len(self) > 0

        def __setitem__(self, key, value):
            self.element.set(key, value)

        def clear(self):
            attr = node.getAttributes()
            for key in self.keys():
                attr.removeNamedItem(key)

        def get(self, key, default=None):
            return self.element.get(key, default=default)

        def has_key(self, key):
            return key in self.iterkeys()

        def items(self):
            return self.element.items()

        def iteritems(self):
            attr = self.element._node.getAttributes()
            for i in xrange(attr.getLength()):
                item = attr.item(i)
                name = item.getName()
                if not name.startswith("xmlns"):
                    yield (name, item.getValue())

        def iterkeys(self):
            attr = self.element._node.getAttributes()
            for i in xrange(attr.getLength()):
                name = attr.item(i).getName()
                if not name.startswith("xmlns"):
                    yield name

        def itervalues(self):
            attr = self.element._node.getAttributes()
            for i in xrange(attr.getLength()):
                item = attr.item(i)
                if not item.getName().startswith("xmlns"):
                    yield item.getValue()

        def keys(self):
            return self.element.keys()

        def pop(self, key):
            attr = self.element.get(key)
            if attr is None:
                raise KeyError(key)
            else:
                node.getAttributes().removeNamedItem(key)
                return attr

        def update(self, other):
            for key, value in other.iteritems():
                self.element.set(key, value)

        def values(self):
            return self.element.values()

    class _ElementTree(object):
        @classmethod
        def _create(cls, document):
            self = cls.__new__(cls)
            self._document = document
            return self

        def __init__(self, element):
            self._document = element._node.getOwnerDocument()

        def __copy__(self, *args):
            raise NotImplementedError

        def __deepcopy__(self, *args):
            raise NotImplementedError

        # def __new__(self):
        #     raise NotImplementedError

        def _setroot(self, root):
            raise NotImplementedError

        def find(self, path, namespaces=None):
            raise NotImplementedError

        def findall(self, path, namespaces=None):
            raise NotImplementedError

        def findtext(self, path, default=None, namespaces=None):
            raise NotImplementedError

        def getiterator(self, tag=None, *tags):
            raise NotImplementedError

        def getpath(self, element):
            raise NotImplementedError

        def getroot(self):
            return _Element(self._document.getDocumentElement())

        def iter(self, tag=None, *tags):
            raise NotImplementedError

        def iterfind(self, path, namespaces=None):
            raise NotImplementedError

        def parse(self, source, parser=None, base_url=None):
            raise NotImplementedError

        def relaxng(self, relaxng):
            raise NotImplementedError

        def write(self, f, encoding=None, method="xml", pretty_print=False, xml_declaration=None, with_tail=True, standalone=None, compression=0, exclusive=False, with_comments=True, inclusive_ns_prefixes=None):
            if encoding is not None or method != "xml" or pretty_print is not False or xml_declaration is not None or with_tail is not True or standalone is not None or exclusive is not False or with_comments is not True or inclusive_ns_prefixes is not None:
                raise NotImplementedError

            if compression == 0 and isinstance(f, (basestring, file, File)):
                # direct
                source = DOMSource(self._document.getDocumentElement())
                result = StreamResult(f)
                identityTransformation.transform(source, result)
            else:
                # first to a BAOS
                f2 = ByteArrayOutputStream()
                source = DOMSource(self._document.getDocumentElement())
                result = StreamResult(f2)
                identityTransformation.transform(source, result)

                if compression > 0:
                    bytes = f2.toByteArray()
                    deflater = Deflater(compression)
                    deflater.setInput(bytes)
                    deflater.finish()
                    output = jarray.zeros(2 * len(bytes), "b")
                    length = deflater.deflate(output)
                    output = output[:length]
                else:
                    output = f2.toByteArray()

                if isinstance(f, basestring):
                    open(f, "wb").write(output.tostring())
                else:
                    f.write(output.tostring())

        def write_c14n(self, file, exclusive=False, with_comments=True, compression=0, inclusive_ns_prefixes=None):
            raise NotImplementedError

        def xinclude(self):
            raise NotImplementedError

        def xmlschema(self, xmlschema):
            raise NotImplementedError

        def xpath(self, _path, namespaces=None, extensions=None, smart_strings=True, **_variables):
            raise NotImplementedError

        def xslt(self, _xslt, extensions=None, access_control=None, **_kw):
            raise NotImplementedError

        @property
        def docinfo(self):
            raise NotImplementedError

        @property
        def parser(self):
            raise NotImplementedError

    class ElementTree(_ElementTree): pass

    class _Element(object):
        _prefixGenerationTable = "".join(chr(x + 49) if ord("0") <= x <= ord("9") else chr(x) for x in range(256))

        @classmethod
        def _create(cls, node):
            self = cls.__new__(cls)
            self._node = node
            return self

        def __contains__(self, element):
            for x in self.iterchildren():
                if x == element:
                    return True
            return False

        def __copy__(self):
            return self.__deepcopy__(None)

        def __deepcopy__(self, memo):
            out = self.makeelement(self.tag, attrib=self.attrib)
            for x in self.iterchildren():
                out.append(x.__deepcopy__(memo))
            return out

        def __delitem__(self, x):
            if isinstance(x, (int, long)):
                delete = []
                for i, y in enumerate(self.iterchildren()):
                    if i == x:
                        delete.append(y)
                        break
                if len(delete) == 0:
                    raise IndexError("list index out of range")
            elif isinstance(x, slice):
                delete = self.getchildren()[x]
            else:
                raise TypeError("'%s' object cannot be interpreted as an index" % type(x).__name__)
            for y in delete:
                self._node.removeChild(y._node)

        def __getitem__(self, x):
            if isinstance(x, (int, long)):
                for i, y in enumerate(self.iterchildren()):
                    if i == x:
                        return y
                raise IndexError("list index out of range")
            elif isinstance(x, slice):
                children = self.getchildren()
                return children[x]
            else:
                raise TypeError("'%s' object cannot be interpreted as an index" % type(x).__name__)

        def __iter__(self):
            return self.iterchildren()

        def __len__(self):
            return len(self.getchildren())

        # def __new__(self, *args):
        #     raise NotImplementedError

        # def __nonzero__(self, x):
        #     raise NotImplementedError

        def __repr__(self):
            return "<Element %s at 0x%x>" % (self.tag, id(self))

        def __reversed__(self):
            return self._iterchildren(reversed=True)

        def __setitem__(self, x, value):
            if not isinstance(value, _Element):
                raise TypeError("%r is not an lxml.etree._Element" % value)

            if isinstance(x, (int, long)):
                for i, y in enumerate(self.iterchildren()):
                    if i == x:
                        selfowner = self._node.getOwnerDocument()
                        valueowner = value._node.getOwnerDocument()
                        if selfowner == valueowner:
                            self._node.replaceChild(value._node, y._node)
                        else:
                            imported = selfowner.importNode(value._node, True)
                            self._node.replaceChild(imported, y._node)
                            parent = value.getparent()
                            if parent is not None:
                                parent._node.removeChild(value._node)
                            value._node = imported
                        return
                raise IndexError("list index out of range")
            raise TypeError("'%s' object cannot be interpreted as an index" % type(x).__name__)

        def _init(self):
            pass  # for subclasses

        def addnext(self, element):
            if not isinstance(element, _Element):
                raise TypeError("%r is not an lxml.etree._Element" % element)
            parent = self.getparent()
            if parent is None:
                raise TypeError("Cannot add siblings to the root element")

            selfowner = self._node.getOwnerDocument()
            elementowner = element._node.getOwnerDocument()
            if selfowner == elementowner:
                beforeme = self._node.getNextSibling()
                if beforeme is None:
                    parent._node.appendChild(element._node)
                else:
                    parent._node.insertBefore(element._node, beforeme)
            else:
                imported = selfowner.importNode(element._node, True)
                beforeme = self._node.getNextSibling()
                if beforeme is None:
                    parent._node.appendChild(imported)
                else:
                    parent._node.insertBefore(imported, beforeme)
                elemparent = element.getparent()
                if elemparent is not None:
                    elemparent._node.removeChild(element._node)
                element._node = imported

        def addprevious(self, element):
            if not isinstance(element, _Element):
                raise TypeError("%r is not an lxml.etree._Element" % element)
            parent = self.getparent()
            if parent is None:
                raise TypeError("Cannot add siblings to the root element")

            selfowner = self._node.getOwnerDocument()
            elementowner = element._node.getOwnerDocument()
            if selfowner == elementowner:
                parent._node.insertBefore(element._node, self._node)
            else:
                imported = selfowner.importNode(element._node, True)
                parent._node.insertBefore(imported, self._node)
                elemparent = element.getparent()
                if elemparent is not None:
                    elemparent._node.removeChild(element._node)
                element._node = imported

        def append(self, element):
            if not isinstance(element, _Element):
                raise TypeError("%r is not an lxml.etree._Element" % element)

            selfowner = self._node.getOwnerDocument()
            elementowner = element._node.getOwnerDocument()
            if selfowner == elementowner:
                self._node.appendChild(element._node)
            else:
                imported = selfowner.importNode(element._node, True)
                self._node.appendChild(imported)
                elemparent = element.getparent()
                if elemparent is not None:
                    elemparent._node.removeChild(element._node)
                element._node = imported

        def clear(self):
            while self._node.hasChildNodes():
                self._node.removeChild(self._node.getFirstChild())
            self._node.getAttributes().removeAll()

            parent = self._node.getParentNode()
            nextSibling = self._node.getNextSibling()
            if parent is not None and nextSibling is not None and nextSibling.getNodeType() == Node.TEXT_NODE:
                parent.removeChild(nextSibling)

        def extend(self, elements):
            for x in elements:
                self.append(x)

        def _clarkToPrefix(self, path, existingNamespaces=None):
            namespaces = {}
            counter = 0
            for ns in re.findall("\{([^}]+)\}", path):
                prefix = str(counter).translate(self._prefixGenerationTable)
                counter += 1
                while existingNamespaces is not None and prefix in existingNamespaces:
                    prefix = str(counter).translate(self._prefixGenerationTable)
                    counter += 1
                namespaces[prefix] = ns

            for prefix, ns in namespaces.items():
                path = path.replace("{%s}" % ns, prefix + ":")

            return path, namespaces

        def find(self, path, namespaces=None):
            x = self.findall(path, namespaces=namespaces)
            if len(x) > 0:
                return x[0]
            else:
                return None

        def findall(self, path, namespaces=None):
            path, newNamespaces = self._clarkToPrefix(path, namespaces)
            if namespaces is None:
                namespaces = newNamespaces
            else:
                namespaces = dict(namespaces)
                namespaces.update(newNamespaces)

            return self.xpath(path, namespaces=namespaces)

        def findtext(self, path, default=None, namespaces=None):
            x = self.find(path, namespaces=namespaces)
            if x is None or x.text is None:
                return default
            else:
                return x.text

        def get(self, key, default=None):
            attr = self._node.getAttributes().getNamedItem(key)
            if attr is None:
                return default
            else:
                return attr.getValue()

        def getchildren(self):
            out = []
            nodes = self._node.getChildNodes()
            for i in xrange(nodes.getLength()):
                item = nodes.item(i)
                if item.getNodeType() == Node.ELEMENT_NODE:
                    out.append(_Element._create(item))
            return out

        def getiterator(self, tag=None):
            return self.iter(tag)

        def getnext(self):
            pointer = self._node.getNextSibling()
            if pointer is None:
                return None
            while pointer.getNodeType() != Node.ELEMENT_NODE:
                pointer = pointer.getNextSibling()
                if pointer is None:
                    return None
            return _Element._create(pointer)

        def getparent(self):
            out = self._node.getParentNode()
            if out is None or out.getNodeType() != Node.ELEMENT_NODE:
                return None
            else:
                return _Element._create(out)

        def getprevious(self):
            pointer = self._node.getPreviousSibling()
            if pointer is None:
                return None
            while pointer.getNodeType() != Node.ELEMENT_NODE:
                pointer = pointer.getPreviousSibling()
                if pointer is None:
                    return None
            return _Element._create(pointer)

        def getroottree(self):
            return _ElementTree._create(self._node.getOwnerDocument())

        def __eq__(self, other):
            if isinstance(other, _Element):
                return self._node.isSameNode(other._node)
            else:
                return False

        def index(self, child, start=None, stop=None):
            for i, x in enumerate(self.iterchildren()):
                if x == child:
                    return i
            raise ValueError("Element is not a child of this node.")

        def insert(self, index, element):
            if not isinstance(element, _Element):
                raise TypeError("%r is not an lxml.etree._Element" % element)
            if not isinstance(index, (int, long)):
                raise TypeError("'%s' object cannot be interpreted as an index" % type(index).__name__)

            for i, x in enumerate(self.iterchildren()):
                if i == index:
                    selfowner = self._node.getOwnerDocument()
                    elementowner = element._node.getOwnerDocument()
                    if selfowner == elementowner:
                        self._node.insertBefore(element._node, x._node)
                    else:
                        imported = selfowner.importNode(element._node, True)
                        self._node.insertBefore(imported, x._node)
                        parent = element.getparent()
                        if parent is not None:
                            parent._node.removeChild(element._node)
                        element._node = imported
                    return
            raise IndexError("list index out of range")

        def items(self):
            out = []
            attr = self._node.getAttributes()
            for i in xrange(attr.getLength()):
                item = attr.item(i)
                name = item.getName()
                if not name.startswith("xmlns"):
                    out.append((name, item.getValue()))
            return out

        def _iter(self):
            yield self
            for x in self.iterchildren():
                for y in x.iter():
                    yield y

        def iter(self, tag=None):
            for x in self._iter():
                if tag is None or x.tag == tag:
                    yield x

        def _iterancestors(self):
            parent = self.getparent()
            if parent is not None:
                yield parent
                for y in parent.iterancestors():
                    yield y

        def iterancestors(self, tag=None):
            for x in self._iterancestors():
                if tag is None or x.tag == tag:
                    yield x

        def _iterchildren(self, reversed=False):
            nodes = self._node.getChildNodes()
            if reversed:
                walker = xrange(nodes.getLength() - 1, -1, -1)
            else:
                walker = xrange(nodes.getLength())
            for i in walker:
                item = nodes.item(i)
                if item.getNodeType() == Node.ELEMENT_NODE:
                    yield _Element._create(item)

        def iterchildren(self, tag=None, reversed=False):
            for x in self._iterchildren(reversed=reversed):
                if tag is None or x.tag == tag:
                    yield x

        def iterdescendants(self, tag=None):
            for x in self._iter():
                if x != self and tag is None or x.tag == tag:
                    yield x

        def iterfind(self, path, namespaces=None):
            for x in self.findall(path, namespaces=namespaces):
                yield x

        def _itersiblings(self):
            pointer = self._node.getNextSibling()
            while pointer is not None:
                if pointer.getNodeType() == Node.ELEMENT_NODE:
                    yield _Element._create(pointer)
                pointer = pointer.getNextSibling()

        def _itersiblings_preceding(self):
            pointer = self._node.getPreviousSibling()
            while pointer is not None:
                if pointer.getNodeType() == Node.ELEMENT_NODE:
                    yield _Element._create(pointer)
                pointer = pointer.getPreviousSibling()

        def itersiblings(self, tag=None, preceding=False):
            if preceding:
                walker = self._itersiblings_preceding()
            else:
                walker = self._itersiblings()
            for x in walker:
                if tag is None or x.tag == tag:
                    yield x

        def itertext(self, tag=None, with_tail=True):
            if tag is None or self.tag == tag:
                if self.text is not None:
                    yield self.text
            for child in self.iterchildren():
                for x in child.itertext(tag, with_tail):
                    yield x
            if tag is None or self.tag == tag:
                if with_tail and self.tail is not None:
                    yield self.tail

        def keys(self):
            out = []
            attr = self._node.getAttributes()
            for i in xrange(attr.getLength()):
                name = attr.item(i).getName()
                if not name.startswith("xmlns"):
                    out.append(name)
            return out

        def makeelement(self, _tag, attrib=None, nsmap=None, **_extra):
            if nsmap is not None:
                raise NotImplementedError("nsmap")
            if len(_extra) > 0:
                raise NotImplementedError("_extra")

            x = _Element._create(self._node.getOwnerDocument().createElement(re.sub("\{[^}]+\}", "", _tag)))
            if attrib is not None:
                for name, value in attrib.items():
                    x.set(name, value)
            return x

        def remove(self, element):
            if not isinstance(element, _Element):
                raise TypeError("%r is not an lxml.etree._Element" % element)
            self._node.removeChild(element._node)

        def replace(self, old_element, new_element):
            if not isinstance(old_element, _Element):
                raise TypeError("%r is not an lxml.etree._Element" % old_element)
            if not isinstance(new_element, _Element):
                raise TypeError("%r is not an lxml.etree._Element" % new_element)

            selfowner = self._node.getOwnerDocument()
            elementowner = element._node.getOwnerDocument()
            if selfowner == elementowner:
                try:
                    self._node.replaceChild(new_element._node, old_element._node)
                except DOMException:
                    raise ValueError("Element is not a child of this node.")
            else:
                imported = selfowner.importNode(new_element._node, True)
                try:
                    self._node.replaceChild(imported, old_element._node)
                except DOMException:
                    raise ValueError("Element is not a child of this node.")
                elemparent = new_element.getparent()
                if elemparent is not None:
                    elemparent._node.removeChild(new_element._node)
                new_element._node = imported

        def set(self, key, value):
            self._node.setAttribute(key, value)

        def values(self):
            out = []
            attr = self._node.getAttributes()
            for i in xrange(attr.getLength()):
                item = attr.item(i)
                if not item.getName().startswith("xmlns"):
                    out.append(item.getValue())
            return out

        def xpath(self, _path, namespaces=None, extensions=None, smart_strings=True, **_variables):
            if extensions is not None:
                raise NotImplementedError("xpath extensions")
            if len(_variables) > 0:
                raise NotImplementedError("xpath variables")

            xpathObject = xpathFactory.newXPath()

            if namespaces is not None:
                class MyNamespaceContext(NamespaceContext):
                    def getNamespaceURI(self, prefix):
                        return namespaces.get(prefix, namespaces.get(None, XMLConstants.NULL_NS_URI))
                    def getPrefix(self, ns):
                        for name, value in namespaces.items():
                            if ns == value:
                                return name
                        return None
                    def getPrefixes(self, ns):
                        return None

                xpathObject.setNamespaceContext(MyNamespaceContext())

            try:
                nodeSet = xpathObject.evaluate(_path, self._node, XPathConstants.NODESET)
            except XPathExpressionException:
                raise XPathEvalError()

            out = []
            for i in xrange(nodeSet.getLength()):
                item = nodeSet.item(i)
                itemType = item.getNodeType()
                if itemType == Node.ELEMENT_NODE:
                    out.append(_Element._create(item))

                elif itemType == Node.ATTRIBUTE_NODE:
                    if smart_strings:
                        out.append(_ElementStringResult._create(item))
                    else:
                        out.append(item.getValue())

                elif itemType == Node.TEXT_NODE:
                    if smart_strings:
                        out.append(_ElementStringResult._create(item))
                    else:
                        out.append(item.getNodeValue())

            return out

        @property
        def attrib(self):
            return _Attrib(self)

        @property
        def base(self):
            return self._node.getBaseURI()

        @property
        def nsmap(self):
            out = {}
            attr = self._node.getOwnerDocument().getDocumentElement().getAttributes()
            for i in xrange(attr.getLength()):
                item = attr.item(i)
                name = item.getName()
                if name.startswith("xmlns"):
                    colonIndex = name.find(":")
                    if colonIndex == -1:
                        out[None] = item.getValue()
                    else:
                        out[name[(colonIndex + 1):]] = item.getValue()
            return out

        @property
        def prefix(self):
            return self._node.getPrefix()

        @property
        def sourceline(self):
            return None  # too bad

        @property
        def tag(self):
            return "{%s}%s" % (node.getNamespaceURI(), self._node.getNodeName())

        @property
        def tail(self):
            nextSibling = self._node.getNextSibling()
            if nextSibling is not None and nextSibling.getNodeType() == Node.TEXT_NODE:
                return nextSibling.getNodeValue()
            else:
                return None

        @property
        def text(self):
            firstChild = self._node.getFirstChild()
            if firstChild is not None and firstChild.getNodeType() == Node.TEXT_NODE:
                return firstChild.getNodeValue()
            else:
                return None

    class ElementBase(_Element): pass

    # def parse(source, parser=None, base_url=None):
    #     Return an ElementTree object loaded with source elements.  If no parser
    #     is provided as second argument, the default parser is used.

    #     The ``source`` can be any of the following:

    #     - a file name/path
    #     - a file object
    #     - a file-like object
    #     - a URL using the HTTP or FTP protocol

    #     To parse from a string, use the ``fromstring()`` function instead.

    #     Note that it is generally faster to parse from a file path or URL
    #     than from an open file object or file-like object.  Transparent
    #     decompression from gzip compressed sources is supported (unless
    #     explicitly disabled in libxml2).

    #     The ``base_url`` keyword allows setting a URL for the document
    #     when parsing from a file-like object.  This is needed when looking
    #     up external entities (DTD, XInclude, ...) with relative paths.

    # def tostring(element_or_tree, encoding=None, method="xml", xml_declaration=None, pretty_print=False, with_tail=True, standalone=None, doctype=None, exclusive=False, with_comments=True):

    #     Serialize an element to an encoded string representation of its XML
    #     tree.

    #     Defaults to ASCII encoding without XML declaration.  This
    #     behaviour can be configured with the keyword arguments 'encoding'
    #     (string) and 'xml_declaration' (bool).  Note that changing the
    #     encoding to a non UTF-8 compatible encoding will enable a
    #     declaration by default.

    #     You can also serialise to a Unicode string without declaration by
    #     passing the ``unicode`` function as encoding (or ``str`` in Py3),
    #     or the name 'unicode'.  This changes the return value from a byte
    #     string to an unencoded unicode string.

    #     The keyword argument 'pretty_print' (bool) enables formatted XML.

    #     The keyword argument 'method' selects the output method: 'xml',
    #     'html', plain 'text' (text content without tags) or 'c14n'.
    #     Default is 'xml'.

    #     The ``exclusive`` and ``with_comments`` arguments are only used
    #     with C14N output, where they request exclusive and uncommented
    #     C14N serialisation respectively.

    #     Passing a boolean value to the ``standalone`` option will output
    #     an XML declaration with the corresponding ``standalone`` flag.

    #     The ``doctype`` option allows passing in a plain string that will
    #     be serialised before the XML tree.  Note that passing in non
    #     well-formed content here will make the XML output non well-formed.
    #     Also, an existing doctype in the document tree will not be removed
    #     when serialising an ElementTree instance.

    #     You can prevent the tail text of the element from being serialised
    #     by passing the boolean ``with_tail`` option.  This has no impact
    #     on the tail text of children, which will always be serialised.

    # def fromstring(text, parser=None, base_url=None):

    #     Parses an XML document or fragment from a string.  Returns the
    #     root node (or the result returned by a parser target).

    #     To override the default parser with a different parser you can pass it to
    #     the ``parser`` keyword argument.

    #     The ``base_url`` keyword argument allows to set the original base URL of
    #     the document to support relative Paths when looking up external entities
    #     (DTD, XInclude, ...).

    # def iterwalk(element_or_tree, events=("end",), tag=None):
    #  |  
    #  |  A tree walker that generates events from an existing tree as if it
    #  |  was parsing XML data with ``iterparse()``.
    #  |  
    #  |  Methods defined here:
    #  |  
    #  |  __init__(...)
    #  |      x.__init__(...) initializes x; see help(type(x)) for signature
    #  |  
    #  |  __iter__(...)
    #  |      x.__iter__() <==> iter(x)
    #  |  
    #  |  __next__(...)
    #  |  
    #  |  next(...)
    #  |      x.next() -> the next value, or raise StopIteration
    #  |  
    #  |  ----------------------------------------------------------------------
    #  |  Data and other attributes defined here:
    #  |  
    #  |  __new__ = <built-in method __new__ of type object>
    #  |      T.__new__(S, ...) -> a new object with type S, a subtype of T
    #  |  
    #  |  __pyx_vtable__ = <capsule object NULL>

    # def iterparse(self, source, events=("end",), tag=None, attribute_defaults=False, dtd_validation=False, load_dtd=False, no_network=True, remove_blank_text=False, remove_comments=False, remove_pis=False, encoding=None, html=False, huge_tree=False, schema=None):
    # Incremental parser.
    #  |  
    #  |  Parses XML into a tree and generates tuples (event, element) in a
    #  |  SAX-like fashion. ``event`` is any of 'start', 'end', 'start-ns',
    #  |  'end-ns'.
    #  |  
    #  |  For 'start' and 'end', ``element`` is the Element that the parser just
    #  |  found opening or closing.  For 'start-ns', it is a tuple (prefix, URI) of
    #  |  a new namespace declaration.  For 'end-ns', it is simply None.  Note that
    #  |  all start and end events are guaranteed to be properly nested.
    #  |  
    #  |  The keyword argument ``events`` specifies a sequence of event type names
    #  |  that should be generated.  By default, only 'end' events will be
    #  |  generated.
    #  |  
    #  |  The additional ``tag`` argument restricts the 'start' and 'end' events to
    #  |  those elements that match the given tag.  By default, events are generated
    #  |  for all elements.  Note that the 'start-ns' and 'end-ns' events are not
    #  |  impacted by this restriction.
    #  |  
    #  |  The other keyword arguments in the constructor are mainly based on the
    #  |  libxml2 parser configuration.  A DTD will also be loaded if validation or
    #  |  attribute default values are requested.
    #  |  
    #  |  Available boolean keyword arguments:
    #  |   - attribute_defaults: read default attributes from DTD
    #  |   - dtd_validation: validate (if DTD is available)
    #  |   - load_dtd: use DTD for parsing
    #  |   - no_network: prevent network access for related files
    #  |   - remove_blank_text: discard blank text nodes
    #  |   - remove_comments: discard comments
    #  |   - remove_pis: discard processing instructions
    #  |   - strip_cdata: replace CDATA sections by normal text content (default: True)
    #  |   - compact: safe memory for short text content (default: True)
    #  |   - resolve_entities: replace entities by their text value (default: True)
    #  |   - huge_tree: disable security restrictions and support very deep trees
    #  |                and very long text content (only affects libxml2 2.7+)
    #  |  
    #  |  Other keyword arguments:
    #  |   - encoding: override the document encoding
    #  |   - schema: an XMLSchema to validate against
    #  |  
    #  |  Method resolution order:
    #  |      iterparse
    #  |      _BaseParser
    #  |      __builtin__.object

    # def XMLParser(self, encoding=None, attribute_defaults=False, dtd_validation=False, load_dtd=False, no_network=True, ns_clean=False, recover=False, XMLSchema schema=None, remove_blank_text=False, resolve_entities=True, remove_comments=False, remove_pis=False, strip_cdata=True, target=None, compact=True):
    #     The XML parser.
    #  |  
    #  |  Parsers can be supplied as additional argument to various parse
    #  |  functions of the lxml API.  A default parser is always available
    #  |  and can be replaced by a call to the global function
    #  |  'set_default_parser'.  New parsers can be created at any time
    #  |  without a major run-time overhead.
    #  |  
    #  |  The keyword arguments in the constructor are mainly based on the
    #  |  libxml2 parser configuration.  A DTD will also be loaded if DTD
    #  |  validation or attribute default values are requested (unless you
    #  |  additionally provide an XMLSchema from which the default
    #  |  attributes can be read).
    #  |  
    #  |  Available boolean keyword arguments:
    #  |  
    #  |  - attribute_defaults - inject default attributes from DTD or XMLSchema
    #  |  - dtd_validation     - validate against a DTD referenced by the document
    #  |  - load_dtd           - use DTD for parsing
    #  |  - no_network         - prevent network access for related files (default: True)
    #  |  - ns_clean           - clean up redundant namespace declarations
    #  |  - recover            - try hard to parse through broken XML
    #  |  - remove_blank_text  - discard blank text nodes
    #  |  - remove_comments    - discard comments
    #  |  - remove_pis         - discard processing instructions
    #  |  - strip_cdata        - replace CDATA sections by normal text content (default: True)
    #  |  - compact            - safe memory for short text content (default: True)
    #  |  - resolve_entities   - replace entities by their text value (default: True)
    #  |  - huge_tree          - disable security restrictions and support very deep trees
    #  |                         and very long text content (only affects libxml2 2.7+)
    #  |  
    #  |  Other keyword arguments:
    #  |  
    #  |  - encoding - override the document encoding
    #  |  - target   - a parser target object that will receive the parse events
    #  |  - schema   - an XMLSchema to validate against
    #  |  
    #  |  Note that you should avoid sharing parsers between threads.  While this is
    #  |  not harmful, it is more efficient to use separate parsers.  This does not
    #  |  apply to the default parser.
    #  |  
    #  |  Method resolution order:
    #  |      XMLParser
    #  |      _FeedParser
    #  |      _BaseParser
    #  |      __builtin__.object

    # def XMLSchema(self, etree=None, file=None):
    #  |  Turn a document into an XML Schema validator.
    #  |  
    #  |  Either pass a schema as Element or ElementTree, or pass a file or
    #  |  filename through the ``file`` keyword argument.
    #  |  
    #  |  Passing the ``attribute_defaults`` boolean option will make the
    #  |  schema insert default/fixed attributes into validated documents.
    #  |  
    #  |  Method resolution order:
    #  |      XMLSchema
    #  |      _Validator
    #  |      __builtin__.object
    #  |  
    #  |  Methods defined here:
    #  |  
    #  |  __call__(...)
    #  |      __call__(self, etree)
    #  |      
    #  |      Validate doc using XML Schema.
    #  |      
    #  |      Returns true if document is valid, false if not.
    #  |  
    #  |  __init__(...)
    #  |      x.__init__(...) initializes x; see help(type(x)) for signature

    # def XSLT(self, xslt_input, extensions=None, regexp=True, access_control=None):
    #  |  
    #  |  Turn an XSL document into an XSLT object.
    #  |  
    #  |  Calling this object on a tree or Element will execute the XSLT::
    #  |  
    #  |    >>> transform = etree.XSLT(xsl_tree)
    #  |    >>> result = transform(xml_tree)
    #  |  
    #  |  Keyword arguments of the constructor:
    #  |  
    #  |  - extensions: a dict mapping ``(namespace, name)`` pairs to
    #  |    extension functions or extension elements
    #  |  - regexp: enable exslt regular expression support in XPath
    #  |    (default: True)
    #  |  - access_control: access restrictions for network or file
    #  |    system (see `XSLTAccessControl`)
    #  |  
    #  |  Keyword arguments of the XSLT call:
    #  |  
    #  |  - profile_run: enable XSLT profiling (default: False)
    #  |  
    #  |  Other keyword arguments of the call are passed to the stylesheet
    #  |  as parameters.

    # def ElementNamespaceClassLookup(self, fallback=None):
    #  |  
    #  |  Element class lookup scheme that searches the Element class in the
    #  |  Namespace registry.
    #  |  
    #  |  Method resolution order:
    #  |      ElementNamespaceClassLookup
    #  |      FallbackElementClassLookup
    #  |      ElementClassLookup
    #  |      __builtin__.object
    #  |  
    #  |  Methods defined here:
    #  |  
    #  |  __init__(...)
    #  |      x.__init__(...) initializes x; see help(type(x)) for signature
    #  |  
    #  |  get_namespace(...)
    #  |      get_namespace(self, ns_uri)
    #  |      
    #  |      Retrieve the namespace object associated with the given URI.
    #  |      Pass None for the empty namespace.
    #  |      
    #  |      Creates a new namespace object if it does not yet exist.

    node = documentBuilder.parse(File("/home/pivarski/odg/pmmlx/trunk/augustus6/test.pmml")).getDocumentElement()
    element = _Element._create(node)

    node2 = documentBuilder.parse(File("/home/pivarski/odg/pmmlx/trunk/augustus6/test.pmml")).getDocumentElement()
    element2 = _Element._create(node2)

except ImportError:
    pass
