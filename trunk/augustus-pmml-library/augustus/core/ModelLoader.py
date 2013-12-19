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

"""This module defines the ModelLoader class."""

import sys
import os
import json
import copy
import inspect
import gzip
try:
    from cStringIO import StringIO
except ImportError:
    try:
        from StringIO import StringIO
    except ImportError:
        from io import BytesIO as StringIO

from lxml.etree import parse, tostring, fromstring, iterwalk, iterparse, XMLParser, ElementTree, XMLSchema, XSLT, ElementNamespaceClassLookup
from lxml.builder import ElementMaker

from augustus.core.defs import defs
from augustus.core.PmmlBinding import PmmlBinding

class ModelLoader(object):
    """ModelLoader is a tool for unserializing or creating PMML
    models.

    A ModelLoader loader instance can be modified to support strict
    PMML compliance, extended PMML features, or optimized
    implementations of PMML elements.

    The user is encouraged to write new PmmlBinding subclasses and
    register them with a ModelLoader to modify the behavior of PMML or
    make certain functions more efficient for a given context.

    ModelLoader is the only supported way to make new PmmlBinding
    instances: any function that produces PMML must be given a
    ModelLoader.

    @type schema: lxml.etree.Element
    @param schema: Representation of the PMML schema used to interpret new models.
    @type tagToClass: dict
    @param tagToClass: Association of PMML tagnames with Python classes.
    """

    def __init__(self, baseXsdFileName="pmml-4-1.xsd", baseXsltFileName="pmml-4-1.xslt"):
        """Initialize a ModelLoader with a base XSD.

        By default, the XSD is the official 4.1 schema published by the U{Data Mining Group<http://www.dmg.org/v4-1/GeneralStructure.html>}.

        @type baseXsdFileName: string
        @param baseXsdFileName: XSD fileName, either absolute or relative to augustus-pmml-library/augustus/core
        @type baseXsltFileName: string
        @param baseXsltFileName: XSLT fileName; future placeholder for XSLT non-local validation.  Not currently used.
        """

        if not os.path.exists(baseXsdFileName):
            baseXsdFileName = os.path.join(os.path.split(__file__)[0], baseXsdFileName)
        self.schema = parse(open(baseXsdFileName)).getroot()

        # if not os.path.exists(baseXsltFileName):
        #     baseXsltFileName = os.path.join(os.path.split(__file__)[0], baseXsltFileName)
        # self.stylesheet = parse(open(baseXsltFileName)).getroot()

        self.preparedSchema = None
        self.tagToClass = {}

    def copy(self):
        """Return a deep copy of the ModelLoader for the sake of
        building multiple lines of PMML interpretation from the same
        base."""

        return copy.deepcopy(self)

    def __getstate__(self):
        """Used by Pickle to serialize the ModelLoader.

        This serialization includes the entire schema and tag-to-class
        mapping.
        """

        serialization = self.__dict__.copy()
        buff = StringIO()
        ElementTree(serialization["schema"]).write(buff, compression=defs.PICKLE_XML_COMPRESSION)
        serialization["schema"] = buff.getvalue()
        # buff = StringIO()
        # ElementTree(serialization["stylesheet"]).write(buff, compression=defs.PICKLE_XML_COMPRESSION)
        # serialization["stylesheet"] = buff.getvalue()
        serialization["preparedSchema"] = None
        return serialization

    def __setstate__(self, serialization):
        """Used by Pickle to unserialize the ModelLoader.

        This serialization includes the entire schema and tag-to-class
        mapping.
        """

        serialization["schema"] = parse(gzip.GzipFile(fileobj=StringIO(serialization["schema"]))).getroot()
        # serialization["stylesheet"] = parse(gzip.GzipFile(fileobj=StringIO(serialization["stylesheet"]))).getroot()
        self.__dict__ = serialization

        for tag, cls in self.tagToClass.items():
            cls.xsd = self.xsdElement(tag)

    def xsdElement(self, elementName):
        """Return the XSD that defines a given xs:element.

        @type elementName: string
        @param elementName: The name of the element to retrieve.
        @rtype: lxml.etree.Element
        @return: The XSD object.
        @raise LookupError: If C{elementName} is not found in the schema, an error is raised.
        """

        results = self.schema.xpath("//xs:element[@name='%s']" % elementName, namespaces={"xs": defs.XSD_NAMESPACE})
        if len(results) == 0:
            return None
        elif len(results) == 1:
            return results[0]
        else:
            raise LookupError("Element \"%s\" is defined %d times in this modelLoader's schema" % (elementName, len(results)))

    def xsdGroup(self, groupName):
        """Return the XSD that defines a given xs:group.

        @type groupName: string
        @param groupName: The name of the group to retrieve.
        @rtype: lxml.etree.Element
        @return: The XSD object.
        @raise LookupError: If C{groupName} is not found in the schema, an error is raised.
        """

        results = self.schema.xpath("//xs:group[@name='%s']" % groupName, namespaces={"xs": defs.XSD_NAMESPACE})
        if len(results) == 0:
            return None
        elif len(results) == 1:
            return results[0]
        else:
            raise LookupError("Group \"%s\" is defined %d times in this modelLoader's schema" % (groupName, len(results)))

    def xsdRemove(self, oldName):
        """Remove an arbitrary object from the ModelLoader's XSD schema.

        @type oldName: string
        @param oldName: Name of the object to be removed.
        """

        for result in self.schema.xpath("//*[@name='%s']" % oldName, namespaces={"xs": defs.XSD_NAMESPACE}):
            parent = result.getparent()
            index = parent.index(result)
            del parent[index]

    def xsdAppend(self, newXsd):
        """Append an arbitrary object to the ModelLoader's XSD schema.

        @type newXsd: string or lxml.etree.Element
        @param newXsd: New XSD object to append.
        """

        if isinstance(newXsd, basestring):
            newXsd = fromstring(newXsd)
        self.schema.append(newXsd)
        self.preparedSchema = None

    def register(self, tag, cls):
        """Define (or redefine) the class that is instantiated for a
        given tagname.

        If the class has an C{xsd} and/or C{xsdAppend} string as a
        class attribute, this method will replace the ModelLoader's
        schema entry for C{tag} with the version defined by the class.

        If the class does not have an C{xsd} attribute, this method
        attach the ModelLoader's schema entry for C{tag} to the class.

        As a result, the class will always end up with a C{xsd} class
        attribute representing its XSD schema.  This schema fragment is
        expressed as a lxml.etree.Element for programmatic use.

        The currently-registered classes are in the ModelLoader's
        C{tagToClass} dictionary.

        @type tag: string
        @param tag: The tagname to define or redefine.
        @type cls: PmmlBinding subclass
        @param cls: The class to associate with C{tag}.
        """

        oldXsdElement = self.xsdElement(tag)

        if cls.xsd is not None:
            if isinstance(cls.xsd, basestring):
                clsxsd = fromstring(cls.xsd)
            else:
                clsxsd = cls.xsd

            newXsdElements = clsxsd.xpath("//xs:element[@name='%s']" % tag, namespaces={"xs": defs.XSD_NAMESPACE})
            if len(newXsdElements) != 1:
                raise ValueError("Class %s has an xsd member but %d definitions of element \"%s\"" % (cls.__name__, len(newXsdElements), tag))
            else:
                newXsdElement = newXsdElements[0]

            if oldXsdElement is None:
                self.xsdAppend(newXsdElement)

            else:
                parent = oldXsdElement.getparent()
                index = parent.index(oldXsdElement)
                del parent[index]
                parent.insert(index, newXsdElement)

            cls.xsd = copy.deepcopy(newXsdElement)

        else:
            cls.xsd = copy.deepcopy(oldXsdElement)

        if cls.xsdRemove is not None:
            for name in cls.xsdRemove:
                self.xsdRemove(name)

        if cls.xsdAppend is not None:
            preexisting = {}
            for elem in self.schema:
                name = elem.get("name")
                if name is not None:
                    preexisting[name] = elem

            for newXsd in cls.xsdAppend:
                if isinstance(newXsd, basestring):
                    newXsd = fromstring(newXsd)

                name = newXsd.get("name")
                if name in preexisting:
                    parent = preexisting[name].getparent()
                    index = parent.index(preexisting[name])
                    del parent[index]
                    
                self.xsdAppend(newXsd)

        self.preparedSchema = None
        self.tagToClass[tag] = cls

    def xsdAddToGroupChoice(self, groupName, newElementNames):
        """Add to an xs:group's xs:choice block.

        @type groupName: string
        @param groupName: The name of the xs:group.
        @type newElementNames: list of strings or a single string
        @param newElementNames: References to the xs:elements to add to the xs:choice block.
        """

        results = self.schema.xpath("//xs:group[@name='%s']/xs:choice" % groupName, namespaces={"xs": defs.XSD_NAMESPACE})
        if len(results) != 1:
            raise LookupError("Group \"%s\" is defined with a choice block %d times in this modelLoader's schema" % (groupName, len(results)))

        E = ElementMaker(namespace=defs.XSD_NAMESPACE, nsmap={"xs": defs.XSD_NAMESPACE})

        if isinstance(newElementNames, basestring):
            results[0].append(E.element(ref=newElementNames))
        else:
            for newElementName in newElementNames:
                results[0].append(E.element(ref=newElementName))

        self.preparedSchema = None

    def xsdReplaceGroup(self, groupName, newXsd):
        """Replace an xs:group in this ModelLoader's schema.

        @type groupName: string
        @param groupName: The name of the xs:group.
        @type newXsd: string or lxml.etree.Element
        @param newXsd: The new XSD represented as an XML string or an lxml.etree.Element; it must contain an xs:group named C{groupName}.
        """

        oldXsdElement = self.xsdGroup(groupName)
        
        if isinstance(newXsd, basestring):
            newXsd = fromstring(newXsd)

        newXsdElements = newXsd.xpath("//xs:group[@name='%s']" % groupName, namespaces={"xs": defs.XSD_NAMESPACE})
        if len(newXsdElements) != 1:
            raise ValueError("newXsd has %d definitions of group \"%s\"" % (len(newXsdElements), groupName))
        else:
            newXsdElement = newXsdElements[0]

        if oldXsdElement is None:
            self.xsdAppend(newXsdElement)
        else:
            parent = oldXsdElement.getparent()
            index = parent.index(oldXsdElement)
            del parent[index]
            parent.insert(index, newXsdElement)

        self.preparedSchema = None

    def elementMaker(self, prefix=None, **parserOptions):
        """Obtain a factory for making in-memory PMML objects.

        This factory is an lxml ElementMaker, pre-loaded with the PMML
        namespace and this ModelLoader's current tag-to-class
        relationship.  See the lxml documentation for how to use an
        ElementMaker.

        @type prefix: string or None
        @param prefix: A prefix for the PMML namespace.
        @param **parserOptions: Arguments passed to lxml's U{XMLParser<http://lxml.de/api/lxml.etree.XMLParser-class.html>}.
        @rtype: ElementMaker
        @return: The ElementMaker factory.
        @see: The lxml U{ElementMaker documentation<http://lxml.de/api/lxml.builder.ElementMaker-class.html>}, which explains how to use an ElementMaker factory.
        """

        class XmlParser(XMLParser):
            def makeelement(parserSelf, *args, **kwds):
                result = XMLParser.makeelement(parserSelf, *args, **kwds)
                if isinstance(result, PmmlBinding):
                    result.modelLoader = self
                return result

        parser = XmlParser(**parserOptions)
        lookup = ElementNamespaceClassLookup()
        namespace = lookup.get_namespace(defs.PMML_NAMESPACE)
        for xsdElement in self.schema.xpath("xs:element", namespaces={"xs": defs.XSD_NAMESPACE}):
            namespace[xsdElement.attrib["name"]] = PmmlBinding
        namespace.update(self.tagToClass)
        parser.set_element_class_lookup(lookup)

        return ElementMaker(namespace=defs.PMML_NAMESPACE, nsmap={prefix: defs.PMML_NAMESPACE}, makeelement=parser.makeelement)

    def validate(self, pmmlBinding, postValidate=True):
        """Validate a PMML subtree on demand.

        Note that by default, PMML is validated as or just after it is
        loaded.  This command is intended to check an in-memory PMML
        object after it has been changed or created by an algorithm.

        @type pmmlBinding: PmmlBinding
        @param pmmlBinding: The in-memory PMML object to check.
        @type postValidate: bool
        @param postValidate: If True, run post-XSD validation checks.  (Note: very few PmmlBinding subclasses have postValidation tests defined as of May 2013.)
        """

        if self.preparedSchema is None:
            self.preparedSchema = XMLSchema(self.schema)

        self.preparedSchema.assertValid(pmmlBinding)

        if postValidate:
            for event, elem in iterwalk(pmmlBinding, events=("end",), tag="{%s}*" % defs.PMML_NAMESPACE):
                if isinstance(elem, PmmlBinding):
                    elem.postValidate()

    # def validateXslt(self, pmmlBinding):
    #     xslt = XSLT(self.stylesheet)
    #     return xslt(pmmlBinding)

    def look(self, tag=None, showXsd=True, showSource=False, stream=None):
        """An informative representation of the ModelLoader's current
        interpretation of PMML, intended for interactive use.

        @type tag: string or None
        @param tag: If a string, look up information about this tag; if None, display all tags in the tag-to-class dictionary.
        @type showXsd: bool
        @param showXsd: If True, show the XSD that defines a valid C{tag}.
        @type showSource: bool
        @param showSource: If True, show the Python source code that implements C{tag}.
        @type stream: file-like object or None
        @param stream: If None, print to C{sys.stdout}; otherwise, write to the specified stream.
        @rtype: None
        @return: None; human-readable output is written to the console or a specified stream.
        """

        if stream is None:
            stream = sys.stdout

        if tag is None:
            names = sorted(self.schema.xpath("xs:element/@name", namespaces={"xs": defs.XSD_NAMESPACE}))
            index = 0
            while index < len(names):
                for i in xrange(4):
                    if index + i < len(names):
                        if names[index + i] in self.tagToClass:
                            word = "[%s]" % names[index + i]
                        else:
                            word = names[index + i]
                        stream.write("%-25s " % word)
                    else:
                        break

                stream.write(os.linesep)
                index += 4

        else:
            xsd = None
            if showXsd:
                try:
                    xsd = self.xsdElement(tag)

                except LookupError:
                    try:
                        xsd = self.xsdGroup(tag)
                    except LookupError:
                        pass

                if xsd is not None:
                    stream.write(tostring(xsd, pretty_print=True))        

            if showSource:
                cls = self.tagToClass.get(tag)
                if cls is not None:
                    if xsd is not None:
                        stream.write(os.linesep)
                    stream.write(inspect.getsource(cls))

        stream.flush()

    def loadXml(self, data, validate=True, postValidate=True, **parserOptions):
        """Load a PMML model represented as an XML string, fileName,
        URI, or file-like object.

        Note that the XML file or string may be Gzip-compressed.

        @type data: string or file-like object
        @param data: The data to load.
        @type validate: bool
        @param validate: If True, validate the resulting PmmlBinding against this ModelLoader's XSD schema while loading.
        @type postValidate: bool
        @param postValidate: If True, run post-XSD validation checks.  (Note: very few PmmlBinding subclasses have postValidation tests defined as of May 2013.)
        @param **parserOptions: Arguments passed to lxml's U{XMLParser<http://lxml.de/api/lxml.etree.XMLParser-class.html>}.
        @rtype: PmmlBinding
        @return: In-memory PMML object.
        """

        if isinstance(data, basestring):
            if len(data) >= 2 and data[0:2] == "\x1f\x8b":
                data = gzip.GzipFile(fileobj=StringIO(data))
            elif data.find("<") != -1:
                data = StringIO(data)

        if validate:
            if self.preparedSchema is None:
                self.preparedSchema = XMLSchema(self.schema)
            schema = self.preparedSchema
        else:
            schema = None

        newParserOptions = {"schema": schema, "huge_tree": True}
        newParserOptions.update(parserOptions)
        parserOptions = newParserOptions

        parser = XMLParser(**parserOptions)
        lookup = ElementNamespaceClassLookup()
        namespace = lookup.get_namespace(defs.PMML_NAMESPACE)
        for xsdElement in self.schema.xpath("xs:element", namespaces={"xs": defs.XSD_NAMESPACE}):
            namespace[xsdElement.attrib["name"]] = PmmlBinding
        namespace.update(self.tagToClass)
        parser.set_element_class_lookup(lookup)

        # ElementNamespaceClassLookup don't work with iterparse, so we have to parse all at once and then iterwalk
        pmmlBinding = parse(data, parser).getroot()
        pmmlBinding.modelLoader = self

        if postValidate:
            for event, elem in iterwalk(pmmlBinding, events=("end",), tag="{%s}*" % defs.PMML_NAMESPACE):
                if isinstance(elem, PmmlBinding):
                    elem.postValidate()

        return pmmlBinding
    
    def _loadJsonItem(self, tag, data, parser, nsmap):
        """Helper function for C{loadJson}; not for public use."""

        if tag.find(":") == -1:
            prefix = None
        else:
            prefix, tag = tag.split(":")

        pretag = nsmap.get(prefix)
        if pretag is None:
            raise ValueError("This document contains a prefix (\"%s\") not found in the namespace (%r)" % (prefix, nsmap))

        attrib = dict((x[1:], data[x]) for x in data if x.startswith("@"))
        childMap = dict((x, data[x]) for x in data if not x.startswith("@") and not x.startswith("#"))

        item = parser.makeelement("{%s}%s" % (pretag, tag), attrib=attrib, nsmap=nsmap)

        children = {}
        for subtag, childList in childMap.items():
            for childItem in childList:
                number = childItem.get("#")
                if number is None:
                    raise ValueError("Subtag \"%s\" has no \"#\"" % subtag)

                children[number] = self._loadJsonItem(subtag, childItem, parser, nsmap)

        for number in xrange(len(children)):
            child = children.get(number)
            if child is not None:
                item.append(child)

        text = data.get("#text")
        if text is not None:
            item.text = text

        tail = data.get("#tail")
        if tail is not None:
            item.tail = tail

        return item

    def loadJson(self, data, validate=True, postValidate=True, **parserOptions):
        """Load a PMML model represented as a JSON string, fileName,
        dict, or file-like object.

        There is no standard XML-to-JSON specification, so we define
        our own.  Our specification is very similar to U{this
        proposal<http://www.xml.com/pub/a/2006/05/31/converting-between-xml-and-json.html>},
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

        @type data: string, dict, or file-like object
        @param data: The data to load.
        @type validate: bool
        @param validate: If True, validate the resulting PmmlBinding against this ModelLoader's XSD schema after loading.
        @type postValidate: bool
        @param postValidate: If True, run post-XSD validation checks.  (Note: very few PmmlBinding subclasses have postValidation tests defined as of May 2013.)
        @param **parserOptions: Arguments passed to lxml's U{XMLParser<http://lxml.de/api/lxml.etree.XMLParser-class.html>}.
        @rtype: PmmlBinding
        @return: In-memory PMML object.
        @raise ValueError: If the JSON text is malformed or does not represent PMML, an error is raised.
        """

        if hasattr(data, "read"):
            data = json.load(data)
        elif isinstance(data, basestring):
            if os.path.exists(data):
                data = json.load(open(data))
            else:
                data = json.loads(data)

        if not isinstance(data, dict):
            raise ValueError("JSON object must be a mapping at the top level")

        if validate:
            if self.preparedSchema is None:
                self.preparedSchema = XMLSchema(self.schema)
            schema = self.preparedSchema
        else:
            schema = None

        parser = XMLParser(**parserOptions)
        lookup = ElementNamespaceClassLookup()
        namespace = lookup.get_namespace(defs.PMML_NAMESPACE)
        for xsdElement in self.schema.xpath("xs:element", namespaces={"xs": defs.XSD_NAMESPACE}):
            namespace[xsdElement.attrib["name"]] = PmmlBinding
        namespace.update(self.tagToClass)
        parser.set_element_class_lookup(lookup)

        try:
            nsmap = data["#nsmap"]
        except KeyError:
            raise ValueError("JSON object must have a \"#nsmap\" key at the top level")

        if "" in nsmap:
            nsmap[None] = nsmap[""]
            del nsmap[""]
        del data["#nsmap"]
        
        if len(data) != 1:
            raise ValueError("JSON object must have exactly one PMML object at the top level")

        tag = data.keys()[0]
        data = data[tag]
        if not isinstance(data, list) or len(data) != 1:
            raise ValueError("Top-level PMML object must be a list with exactly one item")
        data = data[0]
        
        pmmlBinding = self._loadJsonItem(tag, data, parser, nsmap)

        if validate:
            schema.assertValid(pmmlBinding)

        if postValidate:
            for event, elem in iterwalk(pmmlBinding, events=("end",), tag="{%s}*" % defs.PMML_NAMESPACE):
                if isinstance(elem, PmmlBinding):
                    elem.postValidate()

        return pmmlBinding
