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

"""This module defines the PmmlBinding class."""

import pickle
try:
    from cStringIO import StringIO
except ImportError:
    try:
        from StringIO import StringIO
    except ImportError:
        from io import BytesIO as StringIO

from lxml.etree import ElementTree

from augustus.core.defs import defs
from augustus.core.XmlBinding import XmlBinding

def _PmmlBinding_unserialize(modelLoader, xmlString):
    """Used by Pickle to unserialize the PmmlBinding.
    
    This serialization includes the associated ModelLoader's
    schema and tag-to-class mapping, so that the same classes are
    loaded when the pickled object is reconstituted.  This is not
    guaranteed by XML or JSON serialization.  The ModelLoader is
    only stored once per Pickle string.

    The serialization format for the PMML itself is compressed
    XML, with Gzip compression level 1 (fastest).
    """
    return modelLoader.loadXml(xmlString, validate=False, postValidate=False)
_PmmlBinding_unserialize.__safe_for_unpickling__ = True

class PmmlBinding(XmlBinding):
    """Base class for all in-memory representations of PMML.

    PmmlBinding inherits from lxml.etree.ElementBase and is therefore
    generated on the fly by XML parsing.  Although lxml provides a
    nice interface on its own, PmmlBinding adds a few more useful
    methods.

    The user is encouraged to write new PmmlBinding subclasses and
    register them with a ModelLoader to modify the behavior of PMML or
    make certain functions more efficient for a given context.

    Note that PmmlBinding instances are usually stateless.  Subclasses
    that need to maintain a state (such as Aggregate) can do so
    through a DataTableState entry.

    It is sometimes useful to add cached data to a user-defined
    PmmlBinding subclass for speed.  If you do, be sure to maintain
    this cached state, preferably in a DataTableState entry, because
    lxml deletes and auto-generates PmmlBinding instances on the fly
    when their only references are in PmmlBinding element trees.
    """

    xsd = None
    xsdRemove = None
    xsdAppend = None

    ### find subelements

    def xpath(self, *args, **kwds):
        """Compute an XPath relative to this element.

        This method differs from lxml's C{xpath} only in its treatment
        of namespaces with the keyword argument C{prefix}.  If no
        C{prefix} or C{namespaces} arguments are provided, it will
        assume that the PMML prefix is "pmml".  That is::

            "pmml:MiningModel"

        would match a MiningModel tag in the PMML namespace.  Note
        that prefixes and namespaces are always required for XPath.

        @type prefix: string
        @param prefix: The prefix to represent the PMML namespace.  This argument must be a keyword.
        @see: The U{lxml XPath documentation<http://lxml.de/xpathxslt.html#xpath>}.
        """

        if "namespaces" not in kwds:
            kwds["namespaces"] = {}
        if "prefix" in kwds:
            kwds["namespaces"][kwds["prefix"]] = defs.PMML_NAMESPACE
            del kwds["prefix"]
        else:
            kwds["namespaces"]["pmml"] = defs.PMML_NAMESPACE
        return super(PmmlBinding, self).xpath(*args, **kwds)

    ### copying and serialization

    def __copy__(self):
        """Return a copy of the PmmlBinding.

        Note that this does not copy the associated ModelLoader, if
        one exists.
        """

        if hasattr(self, "modelLoader"):
            modelLoader = self.modelLoader                 # we never really copy the modelLoader; just maintain a reference
            del self.modelLoader
            x = super(PmmlBinding, self).__copy__()
            self.modelLoader = modelLoader
            x.modelLoader = modelLoader
            return x
        else:
            return super(PmmlBinding, self).__copy__()

    def __deepcopy__(self, memo):
        """Return a deep copy of all parts of the PmmlBinding except
        the associated ModelLoader, if one exists.
        """

        if hasattr(self, "modelLoader"):
            modelLoader = self.modelLoader                 # even in so-called "deep" copy
            del self.modelLoader
            x = super(PmmlBinding, self).__deepcopy__(memo)
            self.modelLoader = modelLoader
            x.modelLoader = modelLoader
            return x
        else:
            return super(PmmlBinding, self).__deepcopy__(memo)

    def __reduce__(self):
        """Used by Pickle to serialize the PmmlBinding.
        
        This serialization includes the associated ModelLoader's
        schema and tag-to-class mapping, so that the same classes are
        loaded when the pickled object is reconstituted.  This is not
        guaranteed by XML or JSON serialization.  The ModelLoader is
        only stored once per Pickle string.

        The serialization format for the PMML itself is compressed
        XML, with Gzip compression level 1 (fastest).

        @raise PickleError: If this PmmlBinding does not have a reference to the ModelLoader that made it (e.g. if a Python reference to the object was lost and the object was reconstituted by lxml from its C extension's internal data structure), then it cannot be pickled.  To resolve this problem, simply set C{pmmlBinding.modelLoader} to the desired ModelLoader object.
        """

        try:
            modelLoader = self.modelLoader
        except AttributeError:
            raise pickle.PickleError("PmmlBinding instances can only be pickled if they have a .modelLoader attribute pointing back to the ModelLoader that would reconstitute them")
        buff = StringIO()
        ElementTree(self).write(buff, compression=defs.PICKLE_XML_COMPRESSION)
        return _PmmlBinding_unserialize, (modelLoader, buff.getvalue())

    ### for error messages

    def sourcelineAsString(self):
        """Return the sourceline as a string fragment suitable for error messages.

        @rtype: string
        @return: A string in the form " (line ###)" if C{sourceline} is defined for this element, "" otherwise.
        """

        if self.sourceline is None:
            return ""
        else:
            return " (line %d)" % self.sourceline

    ### overload for additional validity checks

    def postValidate(self):
        """PmmlBinding subclasses may override this method to define post-XSD validity checks."""
        pass

    ### extension of get to query XSD for default or type conversion

    def get(self, key, default=None, defaultFromXsd=False, convertType=False):
        """Get an element attribute.

        @type key: string
        @param key: The attribute name.
        @type default: any
        @param default: The object to return if the attribute does not exist and C{defaultFromXsd} is False.
        @type defaultFromXsd: bool
        @param defaultFromXsd: If True, get the default value from the XSD schema's attribute default, rather than C{default}.
        @type convertType: bool
        @param convertType: If True, use the XSD schema's attribute type to conver the object from a string.
        @rtype: string or any
        @return: If C{convertType} is False, the result is always a string.
        """

        value = super(PmmlBinding, self).get(key, default)

        if defaultFromXsd or convertType:
            xsd = self.xsd
            if xsd is None:
                raise RuntimeError("%s object has no associated XSD" % self.__class__.__name__)

            xsdAttribute = xsd.xpath(".//xs:attribute[@name = '%s']" % key, namespaces={"xs": defs.XSD_NAMESPACE})
            if len(xsdAttribute) != 1:
                raise TypeError("%s has %d attributes named \"%s\" in its XSD schema" % (self.__class__.__name__, len(xsdAttribute), key))
            xsdAttribute = xsdAttribute[0]

        if defaultFromXsd and value is None:
            value = xsdAttribute.get("default")

        if convertType and value is not None:
            xsdType = xsdAttribute.get("type")
            if xsdType is None:
                xsdType = xsdAttribute.xpath(".//xs:restriction/@base", namespaces={"xs": defs.XSD_NAMESPACE})
                if len(xsdType) == 1:
                    xsdType = xsdType[0]
                else:
                    xsdType = None

            if xsdType == "xs:boolean":
                if value in ("true", "1"): return True
                elif value in ("false", "0"): return False
                else:
                    raise ValueError("invalid literal for XML boolean: '%s'" % value)

            elif xsdType in ("NUMBER", "REAL-NUMBER", "PROB-NUMBER", "PERCENTAGE-NUMBER", "xs:decimal", "xs:float", "xs:double"):
                return float(value)

            elif xsdType in ("INT-NUMBER", "xs:byte", "xs:int", "xs:integer", "xs:long", "xs:negativeInteger", "xs:nonNegativeInteger", "xs:nonPositiveInteger", "xs:positiveInteger", "xs:short", "xs:unsignedLong", "xs:unsignedInt", "xs:unsignedShort", "xs:unsignedByte"):
                return int(value)
            
        return value

    ### expand all expandable elements (such as Formula)

    def expand(self, modelLoader):
        """Expand all expandable elements (such as Formula).

        @type modelLoader: ModelLoader
        @param modelLoader: The ModelLoader used to build the expanded contents.
        """

        for child in self.iterchildren():
            if isinstance(child, PmmlBinding):
                child.expand(modelLoader)
