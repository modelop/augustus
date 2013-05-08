"""PMML schema utilities
"""

__copyright__ = """
Copyright (C) 2005-2006  Open Data ("Open Data" refers to
one or more of the following companies: Open Data Partners LLC,
Open Data Research LLC, or Open Data Capital LLC.)

This file is part of Augustus.

Augustus is free software; you can redistribute it and/or
modify it under the terms of the GNU General Public License
as published by the Free Software Foundation; either version 2
of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program; if not, write to the Free Software
Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA
"""

import os
import re
from copy import deepcopy
from cStringIO import StringIO

from augustus.external import etree as simple_etree
from augustus.external.lxml import etree

#################################################################

class PMMLSchema(object):
  """Provide access to multiple versions of the PMML XML Schema

  Default XSD directory is 'xsd' in same directory as this code file.

  Most methods accept a 'version' argument indicating which PMML
  XSD version to use.  The class 'version' argument selects the
  default version to be used unless overridden in a specific call.
  If none of these are used then the latest released version is used.

  The version string is used in several formats (eg. '3-1','3.1','3_1')
  in different places in the XML, and all of these formats are
  acceptable as arguments.  This is converted to a tuple (eg. ('3','1'))
  for internal use.
  """
  _pmml_xsd_regex = re.compile(r'pmml-(?P<version>.*)\.xsd$')
  _pmml_xsd_pat = 'pmml-%s.xsd'
  _pmml_default_version = '3.1'
  _namespace_regex = re.compile(r'({(?P<namespace>.*)})?(?P<tag>.*)$')
  _namespace_pat = '{%s}%s'

  # for convenient reference to importers of this module
  XMLSyntaxError = etree.XMLSyntaxError
  XMLSchemaParseError = etree.XMLSchemaParseError
  XMLSchemaValidateError = etree.XMLSchemaValidateError

  def __init__(self,xsddir=None,version=None):
    self.set_default(version)
    self._xsddir = xsddir or os.path.join(os.path.dirname(__file__),'xsd')
    self._xsdfiles = None	# filenames indexed by version-key
    self._xsdtrees = {}		# parsed XSD files indexed by version-key

  def __str__(self):
    return str(self._xsdfilemap())

  def set_default(self,version=None):
    self._default_version = self._split_version(version or self._pmml_default_version)

  @staticmethod
  def parse_any(source,parser=None):
    '''call etree.parse, automatically handling XML strings, and
      returning more meaningful exception message when possible.
    '''
    filename = None
    if isinstance(source,str):
      if source.lstrip().startswith('<'):
        source = StringIO(source)
      else:
        filename = source
    try:
      out = etree.parse(source,parser=parser)
    except etree.XMLSyntaxError,e:
      if filename is not None:
        e.args = (filename+': '+e.args[0],)
      raise
    return out

  ###############################################################
  # version name/key management

  def version_keys(self):
    '''list available versions in tuple-key format'''
    return sorted(self._xsdfilemap().keys())

  def version_strs(self,sep='-'):
    '''list available versions in string format'''
    return [self._join_version(key,sep=sep) for key in self.version_keys()]

  @staticmethod
  def _split_version(arg): return tuple(re.split(r'[-_.]',arg))

  @staticmethod
  def _join_version(arg,sep='-'): return sep.join(arg)

  def _as_version_key(self,arg):
    '''return arg as normalized version key, converting from string,
      and reverting to default value as needed.
    '''
    if not arg:
      arg = self._default_version
    elif isinstance(arg,str):
      arg = self._split_version(arg)
    return arg

  ###############################################################
  # XSD file discovery

  def _xsdfilemap(self):
    '''mapping of available versions to XSD filename.
      this reads XSD directory once and caches the result.
    '''
    if self._xsdfiles is not None:
      return self._xsdfiles
    self._xsdfiles = out = {}
    for name in os.listdir(self._xsddir):
      m = self._pmml_xsd_regex.match(name)
      if not m:
        continue
      version = m.group('version')
      key = self._split_version(version)
      out[key] = name
    return out

  def _schema_filename(self,version=None):
    '''return XSD filename for given version'''
    version = self._as_version_key(version)
    filename = self._xsdfilemap()[version]
    return os.path.join(self._xsddir,filename)

  def _schema_etree(self,version=None):
    '''return loaded XSD etree, caching result for reuse'''
    version = self._as_version_key(version)
    out = self._xsdtrees.get(version,None)
    if out is not None:
      return out
    filename = self._schema_filename(version)
    out = self.parse_any(filename)
    self._xsdtrees[version] = out
    return out

  def get_raw_schema(self,version=None,keep_namespace=False):
    '''return schema tree, stripped of namespace, for inspection'''
    version = self._as_version_key(version)
    xsdtree = self._schema_etree(version)
    out = self.as_element(xsdtree)
    if not keep_namespace:
      out = self.strip_ns(deepcopy(out))
    return out

  def get_schema(self,version=None):
    '''return schema for given PMML version'''
    version = self._as_version_key(version)
    xsdtree = self._schema_etree(version)
    try:
      out = etree.XMLSchema(xsdtree)
    except (etree.XMLSchemaParseError),e:
      filename = self._schema_filename(version)
      e.args = (filename+': '+e.args[0],)
      raise
    return out

  ###############################################################
  # validation of PMML elements

  def validate(self,arg=None,filename=None,version=None):
    '''validate target against PMML schema'''
    if filename is not None:
      arg = self.parse_any(filename)
    if self.is_pmmltree(arg):
      # top level PMML
      pmml = self.apply_ns(arg.getroot(),version=version)
    else:
      # PMML fragment
      pmml = self.as_element(arg)
      pmml = self.apply_ns(pmml,version=version)
    self._validate(pmml,version=version)
    return pmml

  def _validate(self,pmml,version=None):
    '''given target PMML fragment, get appropriate schema and validate'''
    if version is None:
      try:
        version = pmml.getroot().get('version')
      except:
        pass
    schema = self.get_schema(version=version)
    if not schema.validate(pmml):
      raise etree.XMLSchemaValidateError, self._validation_error(schema)

  def _validation_error(self,schema):
    '''return text of error from schema object'''
    errlog = schema.error_log
    errlist = errlog.filter_from_fatals()
    if not len(errlist):
      errlist = errlog.filter_from_errors()
    out = []
    for err in errlist:
      out2 = []
      if err.filename != '<string>':
        out2.append(err.filename)
      if err.line > 1:
        out2.append('line %s' % str(err.line))
      out2.append(err.message)
      msg = ':'.join(out2)
      out.append(msg)
    if len(out) == 1:
      return out[0]
    out.insert(0,'PMML has %s errors:' % len(out))
    return (os.linesep + '  ').join(out)

  ###############################################################
  # namespace manipulation

  def apply_ns(self,arg,version=None):
    '''apply namespace to arg and children'''
    version = self._as_version_key(version)
    target_ns = self.namespace_url(version)
    root = self.as_element(arg)
    for elem in root.getiterator():
      ns,tag = self.get_ns_tag(elem)
      if ns != target_ns:
        elem.tag = self._namespace_pat % (target_ns,tag)
    return root

  def strip_ns(self,arg):
    '''apply namespace to arg and children'''
    root = self.as_element(arg)
    for elem in root.getiterator():
      ns,tag = self.get_ns_tag(elem)
      if ns is not None:
        elem.tag = tag
    return root

  def get_ns_tag(self,arg):
    arg = self.as_element(arg)
    try:
      m = self._namespace_regex.match(arg.tag)
    except TypeError:
      return (None,None)
    ns,tag = m.group('namespace','tag')
    if not ns: ns = None
    return (ns,tag)

  def namespace_url(self,version=None):
    '''return PMML namespace URL for specified version'''
    version = self._as_version_key(version)
    return 'http://www.dmg.org/PMML-%s' % '_'.join(version)

  ###############################################################
  # helper functions

  def is_pmmltree(self,arg):
    '''test if arg is PMML etree.ElementTree'''
    if hasattr(arg,'docinfo') and \
        isinstance(arg.docinfo,etree.DocInfo) and\
        arg.docinfo.root_name == 'PMML':
      return True
    return False

  def as_element(self,arg):
    '''return arg as lxml.etree element,
      converting from string or ElementTree element as necessary.
    '''
    if etree.iselement(arg):
      return arg
    if isinstance(arg,str):
      if arg.lstrip().startswith('<'):
        source = StringIO(arg)
      else:
        # just pass filename through
        source = arg
    else:
      # assume original ElementTree object
      if simple_etree.iselement(arg):
        arg = simple_etree.ElementTree(arg)
      source = StringIO()
      arg.write(source)
      source.seek(0)
    parser = etree.XMLParser(ns_clean=True,remove_blank_text=True)
    return etree.parse(source,parser).getroot()

  def make_root(self,version=None):
    '''create PMML root element for specified version'''
    version = self._as_version_key(version)
    namespace = self.namespace_url(version)
    kwargs = dict(
      version = '.'.join(version),
      #xmlns = namespace,
      nsmap = {None:namespace},
    )
    return etree.Element('PMML',**kwargs)


#################################################################

class PMMLFactory(object):
  def __init__(self,version=None):
    self.schema = schema = PMMLSchema(version=version)
    self.root = schema.make_root()
    self.namespace = ns = schema.namespace_url()
    self.elemargs = dict(
      nsmap = {None:ns},
    )

  def Element(self,*args,**kwargs):
    #kwargs.update(self.elemargs)
    return etree.Element(*args,**kwargs)

  def SubElement(self,*args,**kwargs):
    return etree.SubElement(*args,**kwargs)

  def tostring(self,element=None,pretty_print=True,xml_declaration=False):
    if element is None:
      element = self.root
    return etree.tostring(element,pretty_print=pretty_print,xml_declaration=xml_declaration)

  def write(self,filename,element=None,pretty_print=True,xml_declaration=True):
    out = self.tostring(element=element,pretty_print=pretty_print,xml_declaration=xml_declaration)
    open(filename,'wb').write(out)

  def validate(self,arg=None,filename=None):
    if arg is None and filename is None:
      arg = self.root
    return self.schema.validate(arg=arg,filename=filename)

  def strip_ns(self,arg):
    return self.schema.strip_ns(arg)
    

#################################################################
#################################################################
_test_pmml = {
  '1. Full PMML with namespace': """
<PMML xmlns="http://www.dmg.org/PMML-3_1" version="3.1">
  <Header copyright="Copyright Open Data Group, 2006, All rights reserved" />
  <DataDictionary>
    <DataField name="score" optype="categorical" dataType="integer" />
  </DataDictionary>
</PMML>
  """,
  '2. Full PMML without namespace': """
<PMML version="3.1">
  <Header copyright="Copyright Open Data Group, 2006, All rights reserved" />
  <DataDictionary>
    <DataField name="score" optype="categorical" dataType="integer" />
  </DataDictionary>
</PMML>
  """,
  '3. PMML fragment with namespace': """
<DataDictionary xmlns="http://www.dmg.org/PMML-3_1">
  <DataField name="score" optype="categorical" dataType="integer" />
</DataDictionary>
  """,
  '4. PMML fragment without namespace': """
<DataDictionary>
  <DataField name="score" optype="categorical" dataType="integer" />
</DataDictionary>
  """,
  '5. Broken PMML fragment': """
<DataDictionary>
  <DataField optype="categorical" dataType="integer" />
  <FakeDataField name="imaginary" optype="categorical" dataType="integer" />
</DataDictionary>
  """,
}
#################################################################

if __name__ == "__main__":
  import sys,doctest
  doctest.testmod()
  psl = PMMLSchema()
  print '===== testing XSD files ====='
  for version in psl.version_strs():
    print 'testing version: %-13s : ' % version,
    try:
      s = psl.get_schema(version=version)
      print 'OK'
    except:
      print sys.exc_info()[1]

  print '===== testing PMML samples ====='
  for title,pmml in sorted(_test_pmml.items()):
    print 'testing: %-35s : ' % title,
    try:
      psl.validate(pmml)
      print 'OK'
    except etree.XMLSchemaValidateError:
      print sys.exc_info()[1]

  if len(sys.argv) > 1:
    print '===== testing command line arg files ====='
  for arg in sys.argv[1:]:
    print 'testing file: %s : ' % arg,
    try:
      psl.validate(filename=arg)
      print 'OK'
    except etree.XMLSchemaValidateError:
      print sys.exc_info()[1]
      
  #print etree.tostring(psl.make_root())

#################################################################
