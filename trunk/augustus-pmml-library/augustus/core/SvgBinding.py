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

"""This module defines the SvgBinding class."""

import os
import codecs
try:
    from cStringIO import StringIO
except ImportError:
    try:
        from StringIO import StringIO
    except ImportError:
        from io import BytesIO as StringIO

from lxml.etree import ElementTree, XMLParser, ElementDefaultClassLookup, parse
from lxml.builder import ElementMaker

from augustus.core.defs import defs
from augustus.core.XmlBinding import XmlBinding

class SvgBinding(XmlBinding):
    """Class for all in-memory representations of SVG.

    Plotting elements create graphics as an SvgBinding before
    serializing them to SVG strings for viewing.  The user can
    intercept this process and manipulate the SvgBinding in the same
    way as PMML.  This may be useful for tweaking a plot or adding
    annotations that are not representable with the standard Augustus
    tools.
    
    SvgBinding objects are also useful as inputs to some plotting
    elements.  Examples include a watermark or logo under a plot,
    using pictograms in a scatter plot, or drawing annotations on top
    of a plot.
    """

    ### find subelements

    def xpath(self, *args, **kwds):
        """Compute an XPath relative to this element.

        This method differs from lxml's C{xpath} only in its treatment
        of namespaces with the keyword argument C{prefix}.  If no
        C{prefix} or C{namespaces} arguments are provided, it will
        assume that the SVG prefix is "svg".  That is::

            "svg:rect"

        would match a rect tag in the SVG namespace.  Note that
        prefixes and namespaces are always required for XPath.

        @type prefix: string
        @param prefix: The prefix to represent the SVG namespace.  This argument must be a keyword.
        @see: The U{lxml XPath documentation<http://lxml.de/xpathxslt.html#xpath>}.
        """

        if "namespaces" not in kwds:
            kwds["namespaces"] = {}
        if "prefix" in kwds:
            kwds["namespaces"][kwds["prefix"]] = defs.SVG_NAMESPACE
            del kwds["prefix"]
        else:
            kwds["namespaces"]["svg"] = defs.SVG_NAMESPACE
        return super(SvgBinding, self).xpath(*args, **kwds)

    ### view the graphic

    def view(self):
        """View the SVG object in Augustus's compiled svgviewer, if available.

        @raise RuntimeError: If C{augustus.svgviewer} has not been compiled or installed, this fails with instructions for viewing the SVG in a web browser or installing the C{svgviewer}.
        """

        try:
            from augustus.svgviewer import view
        except ImportError:
            raise RuntimeError("The optional augustus.svgviewer module is required for \"view\" but it hasn't been installed;%sRecommendation (1): use \"xmlFile\" to save the graphic to a file and then view it in a web browser;%sRecommendation (2): re-build Augustus with \"python setup.py install --with-svgviewer\"" % (os.linesep, os.linesep))
        view(self.xml())

    ### read in, write out

    @staticmethod
    def loadXml(data, **parserOptions):
        """Load SVG from an XML string, fileName, or file-like object.

        @type data: string or file-like object
        @param data: The serialized SVG, fileName, or file-like object that generates SVG as XML.
        @param **parserOptions: Arguments passed to lxml's U{XMLParser<http://lxml.de/api/lxml.etree.XMLParser-class.html>}.
        @rtype: SvgBinding
        @return: An in-memory representation of the SVG.
        """

        if isinstance(data, basestring):
            if os.path.exists(data):
                data = open(data)
            else:
                data = StringIO(data)

        newParserOptions = {"huge_tree": True}
        newParserOptions.update(parserOptions)
        parserOptions = newParserOptions

        parser = XMLParser(**parserOptions)
        lookup = ElementDefaultClassLookup(element=SvgBinding)
        parser.set_element_class_lookup(lookup)

        return parse(data, parser).getroot()

    @staticmethod
    def toXmlFile(elem, file, *args, **kwds):
        """Save SVG to a file or file-like stream.

        @type elem: SvgBinding
        @param elem: The in-memory representation of the SVG.
        @type file: string or file-like object
        @param file: If a string, write to that fileName; if a file-like object, write to that stream.
        @param *args, **kwds: Arguments passed to lxml's U{ElementTree.write<http://lxml.de/api/lxml.etree._ElementTree-class.html#write>}
        """

        if isinstance(file, basestring):
            file = codecs.open(file, "w", "utf-8")
        file.write(defs.SVG_FILE_HEADER)
        ElementTree(elem).write(file, *args, **kwds)

def makeElementMaker():
    """Obtain a factory for making in-memory SVG objects.

    This factory is an lxml ElementMaker, pre-loaded with the SVG
    namespace and this ModelLoader's current tag-to-class
    relationship.  See the lxml documentation for how to use an
    ElementMaker.

    The C{SvgBinding} class has an C{elementMaker} attribute that
    should be used instead of calling this function.

    @see: The lxml U{ElementMaker documentation<http://lxml.de/api/lxml.builder.ElementMaker-class.html>}, which explains how to use an ElementMaker factory.
    """

    parser = XMLParser(huge_tree=True)
    lookup = ElementDefaultClassLookup(element=SvgBinding)
    parser.set_element_class_lookup(lookup)

    return ElementMaker(namespace=defs.SVG_NAMESPACE, nsmap={None: defs.SVG_NAMESPACE, "xlink": defs.XLINK_NAMESPACE}, makeelement=parser.makeelement)

SvgBinding.elementMaker = makeElementMaker()
