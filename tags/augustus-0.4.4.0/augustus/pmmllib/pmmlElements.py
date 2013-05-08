"""These classes represent the basic pmml elements"""

__copyright__ = """
Copyright (C) 2006-2010  Open Data ("Open Data" refers to
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


from pmmlBases import *
import os
import numpy
import operator
from augustus.runlib.execRow import *

########################################################################
#error class used to indicate that a field is missing from a given event
class fieldMissing(StandardError):
  pass

########################################################################
#error strings
class pmmlErrorStrings:
  """this class simply contains all error strings output using a pmmlError"""
  #raised when an error occurred while initializing an element:  identifies the top-level problem
  elementsAndAttributes = "Element not correctly formed(missing/extra elements/attributes)"
  elements = "Element not correctly formed(missing/extra elements)"
  attributes = "Element not correctly formed(missing/extra attributes)"
  noAttributes = "Element contains no attributes"
  noElements = "Element contains no children"
  #raised when a function is utilized improperly
  notDefined = "Function not defined:  "
  arguments = "Function expects parameters numbering exactly:  "
  lengths = "Function expects equally long lists of values.  Received lists of lengths:  "
  #raised when the data dictionary fields can not be input properly
  functionNotDefined = "The function to be used for inputting fields is not defined!"
  typeError = "Data not of given type from field:  "
  fieldNotDefined = "The given field is not defined in the data dictionary:  "
  #this class is used to tell when a testDistributions element is not specific enough to be 
  #used in scoring
  fieldMissing = "The field attribute is missing:" + os.linesep
  alternateMissing = "The alternate distribution is missing:" + os.linesep
  functionUndefined = "The function given is not defined:" + os.linesep

########################################################################
#pmml basic element classes
class pmmlExtension(pmmlElement):
  """Extension element
  used to add functionality that is not in standard PMML"""
  def __init__(self, name="", attributes={}, children=[]):
    """define and instantiate the element"""
    myChild = pmmlList(pmmlTypes, children)
    pmmlElement.__init__(self, "Extension", myChild, ["extender","name","value"], attributes)

class pmmlApplication(pmmlElement):
  """Application element
  used to identify the application name and, possibly, version of the
    application that generated the model"""
  def __init__(self, name="", attributes={}, children=[]):
    """define and instantiate the element"""
    myChild = pmmlList([pmmlExtension], children)
    pmmlElement.__init__(self, "Application", myChild, ["name", "version"], attributes, ["name"])

class pmmlAnnotation(pmmlElement):
  """Annotation element
  used to manually add notes to the model (intended as a model modification history)"""
  def __init__(self, name="", attributes={}, children=[]):
    """define and instantiate the element"""
    if attributes != {}:
      raise pmmlError, "Annotation:  " + pmmlErrorStrings.noAttributes
    myChild = pmmlList([pmmlExtension, pmmlString], children)
    pmmlElement.__init__(self, "Annotation", myChild)

class pmmlTimestamp(pmmlElement):
  """Timestamp element
  used to tell when the model was created"""
  def __init__(self, name="", attributes={}, children=[]):
    """define and instantiate the element"""
    if attributes != {}:
      raise pmmlError, "Timestamp:  " + pmmlErrorStrings.noAttributes
    myChild = pmmlList([pmmlExtension, pmmlString], children)
    pmmlElement.__init__(self, "Timestamp", myChild)

class pmmlHeader(pmmlElement):
  """Header element
  used to contain all information about how the model was created and has been modified"""
  def __init__(self, name="", attributes={}, children=[]):
    """define and instantiate the element"""
    types = [pmmlExtension, pmmlApplication, pmmlAnnotation, pmmlTimestamp]
    maximums = [None, 1, None, 1]
    minimums = [None, None, None, None]
    (extras, children) = pmmlSequence.formatChildren(types, maximums, children)
    if extras != []:
      raise pmmlError, "Header:  " + pmmlErrorStrings.elements
    myChild = pmmlSequence(types, minimums, maximums, children)
    pmmlElement.__init__(self, "Header", myChild, ["copyright","description"], attributes, ["copyright"])

class pmmlMiningBuildTask(pmmlElement):
  """MiningBuildTask element
  used to specify configurations utilized to create the model"""
  def __init__(self, name="", attributes={}, children=[]):
    """define and instantiate the element"""
    if attributes != {}:
      raise pmmlError, "MiningBuildTask:  " + pmmlErrorStrings.noAttributes
    myChild = pmmlList([pmmlExtension], children)
    pmmlElement.__init__(self, "MiningBuildTask", myChild)

class pmmlInterval(pmmlElement):
  """Interval element
  defines a range of numeric values"""
  def __init__(self, name="", attributes={}, children=[]):
    """define and instantiate the element"""
    myChild = pmmlList([pmmlExtension], children)
    pmmlElement.__init__(self, "Interval", myChild, ["closure","leftMargin","rightMargin"], attributes, ["closure"])
  
  def left(self):
    left = self.getAttribute("leftMargin")
    if not left is None:
      return float(left)
    return left
  
  def includeLeft(self):
    """returns true if the left margin is closed"""
    closure = self.getAttributes()["closure"]
    return closure[:6] == "closed"
  
  def right(self):
    right = self.getAttribute("rightMargin")
    if not right is None:
      return float(right)
    return right
  
  def includeRight(self):
    """returns true if the right margin is closed"""
    closure = self.getAttributes()["closure"]
    return closure[len(closure) - 6:] == "Closed"

class pmmlValue(pmmlElement):
  """Value element
  used to represent a value"""
  def __init__(self, name="", attributes={}, children=[]):
    """define and instantiate the element"""
    myChild = pmmlList([pmmlExtension], children)
    pmmlElement.__init__(self, "Value", myChild, ["value","displayValue","property"], attributes, ["value"])

class choiceVI(pmmlChoice):
  """"""
  types = [pmmlValue, pmmlInterval]
  __maximums = [None, None]
  def __init__(self, instances):
    """"""
    minimums = [None, None]
    pmmlChoice.__init__(self, choiceVI.types, minimums, choiceVI.__maximums, instances)
  
  @staticmethod  
  def formatChildren(children):
    """"""
    return pmmlChoice.formatChildren(choiceVI.types, choiceVI.__maximums, children)

NULL = object()

class pmmlDataField(pmmlElement):
  """DataField element
  used to represent an input data field"""
  def __init__(self, name="", attributes={}, children=[]):
    """define and instantiate the element"""
    types = [pmmlExtension, choiceVI]
    maximums = [None, 1]
    minimums = [None, None]
    (extras, children) = pmmlSequence.formatChildren(types, maximums, children)
    if extras != []:
      raise pmmlError, "DataField:  " + pmmlErrorStrings.elements
    myChild = pmmlSequence(types, minimums, maximums, children)
    attributeNames = ["name","displayName","optype","dataType", "taxonomy", "isCyclic"]
    requiredAttributes = ["name", "optype", "dataType"]
    pmmlElement.__init__(self, "DataField", myChild, attributeNames, attributes, requiredAttributes)
  
  def convertString(self, variable, dataType, indent=""):
    return indent + """try:
""" + indent + "  " + variable + " = pmmlString.convert(" + variable + ", '" + dataType + """')
""" + indent + """except:
""" + indent + """  raise pmmlError, "DataField:  " + pmmlErrorStrings.typeError + '""" + variable + "'"
  
  def code(self, get, indent=""):
    name = self.getAttribute("name")
    dataType = self.getAttribute("dataType")
    return indent + """try:
""" + indent + "  " + name + " = get('" + name + "')" + """
""" + indent + """except fieldMissing:
""" + indent + "  " + name + """ = NULL
""" + indent + "if not " + name + " is None and not " + name + """ is NULL:
""" + self.convertString(name, dataType, "  " + indent)
  
  def column(self, get, call):
    name = self.getAttribute("name")
    return execColumn(name, self.code(get), "DataField:  " + name, localDict={"pmmlString":pmmlString,"pmmlErrorStrings":pmmlErrorStrings,"NULL":NULL,"pmmlError":pmmlError,"fieldMissing":fieldMissing,"get":get})
  
  def dependencies(self):
    return []
  
  def initialize(self, get):
    """"""
    self.__get = get
    self.__field = self.getAttribute("name")
    self.__dataType = self.getAttribute("dataType")
  
  def convert(self, value):
    """"""
    try:
      return pmmlString.convert(value, self.__dataType)
    except:
      raise pmmlError, "DataField:  " + pmmlErrorStrings.typeError + self.__field
  
  def get(self):
    """"""
    try:
      value = self.__get(self.__field)
    except fieldMissing:
      return NULL
    if value is None:
      return value
    return self.convert(value)

class pmmlTableLocator(pmmlElement):
  """TableLocator element
  used to locate an outside table from which to import data"""
  def __init__(self, name="", attributes={}, children=[]):
    """define and instantiate the element"""
    if attributes != {}:
      raise pmmlError, "TableLocator:  " + pmmlErrorStrings.noAttributes
    myChild = pmmlList([pmmlExtension], children)
    pmmlElement.__init__(self, "TableLocator", myChild)

class pmmlXMLElement(pmmlElement):
  """xml element
  strictly for use when raw xml must be processed"""
  def __init__(self, name, attributes={}, children=[]):
    """define and instantiate the element"""
    myChild = pmmlList([pmmlXMLElement, pmmlString], children)
    pmmlElement.__init__(self, name, myChild, attributes.keys(), attributes, whiteSpaceImportant=True)

class pmmlrow(pmmlElement):
  """row element
  acts as a row in a database with the xml elements delimiting columns
  i.e.:
  <row><name>Santa Claus</name><occupation>Santa Claus</occupation><row>
  <row><name>Tooth Fairy</name><occupation>Tooth Fairy</occupation><row>
  <row><name>Tom O'Leery</name><occupation>Leprechaun</occupation><row>
  <row><name>Mad Hatter</name><occupation>Crazy Merchant</occupation><row>"""
  def __init__(self, name="", attributes={}, children=[]):
    """define and instantiate the element"""
    if attributes != {}:
      raise pmmlError, "row:  " + pmmlErrorStrings.noAttributes
    myChild = pmmlBoundedList([pmmlXMLElement], 2, None, children)
    pmmlElement.__init__(self, "row", myChild, whiteSpaceImportant=True)
  
  def makeMap(self, inputs, output):
    """returns (input column values, output column value) iff the input columns
    and the output column exist otherwise it returns (None, None)
    
    assumes that all columns contain only values...not other xml elements"""
    children = self.getChildren()
    names = {}
    index = 0
    #form a mapping between this row's column names and their values
    for child in children:
      names[child.getName()] = str(child.getChildren()[0])
      index += 1
    columns = names.keys()
    #form a mapping between the input column names and this row's values
    values = []
    for name in inputs:
      if name in columns:
        values.append(names[name])
      else:
        break
    if len(values) == len(inputs) and output in columns:
      #returns the value of the output column
      return (tuple(values), names[output])
    return (None, None)

class pmmlInlineTable(pmmlElement):
  """InlineTable element
  used to represent a table in the model
  currently, this is implemented utilizing row elements only"""
  def __init__(self, name="", attributes={}, children=[]):
    """define and instantiate the element"""
    types = [pmmlExtension, pmmlrow]
    maximums = [None, None]
    minimums = [None, None]
    if attributes != {}:
      raise pmmlError, "InlineTable:  " + pmmlErrorStrings.noAttributes
    (extras, children) = pmmlSequence.formatChildren(types, maximums, children)
    if extras != []:
      raise pmmlError, "InlineTable:  " + pmmlErrorStrings.elements
    myChild = pmmlSequence(types, minimums, maximums, children)
    pmmlElement.__init__(self, "InlineTable", myChild)
  
  def makeMap(self, columns, outputColumn, dataType):
    """"""
    children = self.getChildrenOfType(pmmlrow)
    #for each row, if it is a valid mapping, map it
    mapped = {}
    for child in children:
      (inputs, output) = child.makeMap(columns, outputColumn)
      if inputs is not None:
        mapped[inputs] = pmmlString.convert(output, dataType)
    return mapped

class choiceTI(pmmlChoice):
  """"""
  types = [pmmlTableLocator, pmmlInlineTable]
  __maximums = [1, 1]
  def __init__(self, instances):
    """"""
    minimums = [1, 1]
    pmmlChoice.__init__(self, choiceTI.types, minimums, choiceTI.__maximums, instances)
  
  @staticmethod    
  def formatChildren(children):
    """"""
    return pmmlChoice.formatChildren(choiceTI.types, choiceTI.__maximums, children)

class pmmlChildParent(pmmlElement):
  """ChildParent element
  used to define hierarchical relationships between fields"""
  def __init__(self, name="", attributes={}, children=[]):
    """define and instantiate the element"""
    types = [pmmlExtension, choiceTI]
    maximums = [None, 1]
    minimums = [None, 1]
    (extras, children) = pmmlSequence.formatChildren(types, maximums, children)
    if extras != []:
      raise pmmlError, "ChildParent:  " + pmmlErrorStrings.elements
    myChild = pmmlSequence(types, minimums, maximums, children)
    pmmlElement.__init__(self, "ChildParent", myChild, ["childField","parentField","parentLevelField","isRecursive"], attributes, ["childField","parentField"])

class pmmlTaxonomy(pmmlElement):
  """Taxonomy element
  used to define a sequence of hierarchical relationships between fields"""
  def __init__(self, name="", attributes={}, children=[]):
    """define and instantiate the element"""
    types = [pmmlExtension, pmmlChildParent]
    maximums = [None, None]
    minimums = [None, None]
    (extras, children) = pmmlSequence.formatChildren(types, maximums, children)
    if extras != []:
      raise pmmlError, "Taxonomy:  " + pmmlErrorStrings.elements
    myChild = pmmlSequence(types, minimums, maximums, children)
    pmmlElement.__init__(self, "Taxonomy", myChild, ["name"], attributes, ["name"])
  
class dictionary(execRow):
  def get(self, field):
    if field in self:
      return self[field]
    return self.__outerGet(field)
  
  def __init__(self, fields, get, call=None, selfReferential=False):
    if not selfReferential:
      self.__get = get
    else:
      self.__outerGet = get
      self.__get = self.get
    columns = []
    self.__dependencies = {}
    self.__datatypes = {}
    self.__optypes = {}
    for field in fields:
      columns.append(field.column(self.__get, call))
      name                      = field.getAttribute("name")
      self.__dependencies[name] = list(set(field.dependencies()))
      self.__datatypes[name]    = field.getAttribute("dataType")
      self.__optypes[name]      = field.getAttribute("optype")
    self.__fields = self.__dependencies.keys()
    execRow.__init__(self, columns)

  def add_data_types(self, datatypes):
    """Add new datatype associations.  Needed for when we extend this dictionary."""
    for name, datatype in datatypes.iteritems():
      self.__datatypes[name] = datatype
  
  def dependencies(self):
    return dict(self.__dependencies)
  
  def datatype(self, field):
    """Returns the datatype of the given field or None if the field does not exist."""
    return self.__datatypes.get(field)
  
  def optype(self, field):
    """Returns the optype of the given field or None if the field does not exist."""
    return self.__optypes.get(field)

class pmmlDataDictionary(pmmlElement):
  """DataDictionary element
  used to define the data input fields"""
  def __init__(self, name="", attributes={}, children=[]):
    """define and instantiate the element"""
    types = [pmmlExtension, pmmlDataField, pmmlTaxonomy]
    maximums = [None, None, None]
    minimums = [None, 1, None]
    (extras, children) = pmmlSequence.formatChildren(types, maximums, children)
    if extras != []:
      raise pmmlError, "DataDictionary:  " + pmmlErrorStrings.elements
    myChild = pmmlSequence(types, minimums, maximums, children)
    pmmlElement.__init__(self, "DataDictionary", myChild, ["numberOfFields"], attributes)
  
  def dictionary(self, get):
    return dictionary(self.getChildrenOfType(pmmlDataField), get)
  
  def columns(self, get):
    return [field.column(get) for field in self.getChildrenOfType(pmmlDataField)]

class pmmlConstant(pmmlElement):
  """Constant element
  used to represent a constant value::
    if a data type is given:
      the value is cast to that type
    else:
      if the value contains a ".", the value is cast to a float
      if the value could be an integer, the value is cast to an integer
      if the value could be a long, the value is cast to a long
      else:
        the value remains a string
  """
  def __init__(self, name="", attributes={}, children=[]):
    """define and instantiate the element"""
    if len(children) != 1:
      raise pmmlError, "Constant:  " + pmmlErrorStrings.elements
    myChild = children[0]
    pmmlElement.__init__(self, "Constant", myChild, ["dataType"], attributes, whiteSpaceImportant=True)
  
  def dependencies(self):
    return []
  
  def code(self, name, indent=""):
    dataType = self.getAttribute("dataType")
    value = self.getChildren()[0]
    if dataType:
      return indent + name + " = pmmlString.convert('" + value + "', '" + dataType + "')" + os.linesep
    else:
      if value.isdigit():
        #Only digits so we treat the value as an integer.
        #I'm not sure we need to convert the value to an int or long each time.
        if value == str(int(value)):
          return indent + name + " = int(" + value + ")" + os.linesep
        else:
          return indent + name + " = long(" + value + ")" + os.linesep
      else:
        try:
          if value.find(".") != -1:
            float(value)
            return indent + name +" = float(" + value + ")" + os.linesep
          else:
            #This is a string
            return indent + name + " = '" + value + "'" + os.linesep
        except ValueError:
          return indent + name + " = '" + value + "'" + os.linesep

class pmmlFieldRef(pmmlElement):
  """FieldRef element
  used as a pointer to a given field
  whatever the value of that field, this acts like that value"""
  def __init__(self, name="", attributes={}, children=[]):
    """define and instantiate the element"""
    myChild = pmmlList([pmmlExtension], children)
    pmmlElement.__init__(self, "FieldRef", myChild, ["field"], attributes, ["field"])
  
  def dependencies(self):
    return [self.getAttribute("field")]
  
  def code(self, name, indent=""):
    return indent + name + " = get('" + self.getAttribute("field") + "')" + os.linesep

class pmmlLinearNorm(pmmlElement):
  """LinearNorm element
  represents a point
  orig is the independent variable's value and norm is the dependent variable's value"""
  def __init__(self, name="", attributes={}, children=[]):
    """define and instantiate the element"""
    myChild = pmmlList([pmmlExtension], children)
    pmmlElement.__init__(self, "LinearNorm", myChild, ["orig","norm"], attributes, ["orig","norm"])

class pmmlNormContinuous(pmmlElement):
  """NormContinuous element
  used to normalize a continuous input field through a function"""
  def __init__(self, name="", attributes={}, children=[]):
    """define and instantiate the element"""
    types = [pmmlExtension, pmmlLinearNorm]
    maximums = [None, None]
    minimums = [None, 2]
    (extras, children) = pmmlSequence.formatChildren(types, maximums, children)
    if extras != []:
      raise pmmlError, "NormContinuous:  " + pmmlErrorStrings.elements
    myChild = pmmlSequence(types, minimums, maximums, children)
    pmmlElement.__init__(self, "NormContinuous", myChild, ["field"], attributes, ["field"])
    self.outliers = None
  
  def dependencies(self):
    return [self.getAttribute("field")]
  
  def code(self, name, indent=""):
    """anything not inside defined lines is missing"""
    coordinates = self.getChildrenOfType(pmmlLinearNorm)
    #extract the first coordinate pair
    attrs = coordinates[0].getAttributes()
    lastX = float(attrs["orig"])
    lastY = float(attrs["norm"])
    #make a line for each additional pair given
    lines = []
    for pair in coordinates[1:]:
      attrs = pair.getAttributes()
      thisX = float(attrs["orig"])
      thisY = float(attrs["norm"])
      #each line is represented as:
      #  [lowest x value, highest x value, slope, f(lowest x value)]
      lines.append([lastX, thisX, (thisY - lastY)/(thisX - lastX), lastY])
      lastX = thisX
      lastY = thisY
    return indent + name + " = get('" + self.getAttribute("field") + "')" + """
""" + indent + "lines = " + str(lines) + """
""" + indent + "if not " + name + """ is NULL:
""" + indent + """  ___missing = True
""" + indent + """  for line in lines:
""" + indent + """    if """ + name + """ >= line[0] and """ + name + """ <= line[1]:
""" + indent + """      """ + name + """ = (""" + name + """ - set[0]) * set[2] + set[3]
""" + indent + """      ___missing = False
""" + indent + """      break
""" + indent + """  if ___missing:
""" + indent + """    """ + name + """ = None
"""

class pmmlNormDiscrete(pmmlElement):
  """NormDiscrete element
  used to normalize a field to a 1 (true) or 0 (false)"""
  def __init__(self, name="", attributes={}, children=[]):
    """define and instantiate the element"""
    myChild = pmmlList([pmmlExtension], children)
    pmmlElement.__init__(self, "NormDiscrete", myChild, ["field","method","value","mapMissingTo"], attributes, ["field","value"])
  
  def dependencies(self):
    return [self.getAttribute("field")]
  
  def code(self, name, indent=""):
    """"""
    return indent + name + " = get('" + self.getAttribute("field") + """')
""" + indent + "if not " + name + """ is NULL:
""" + indent + "  if " + name + """ is None:
""" + indent + "    " + name + " = " + str(self.getAttribute("mapMissingTo")) + """
""" + indent + "  elif " + name + " == '" + self.getAttribute("value") + """':
""" + indent + "    " + name + """ = 1
""" + indent + """  else:
""" + indent + "    " + name + """ = 0
"""
  
class pmmlDiscretizeBin(pmmlElement):
  """DiscretizeBin element
  used to normalize numbers in a range to a single value"""
  def __init__(self, name="", attributes={}, children=[]):
    """define and instantiate the element"""
    types    = [pmmlExtension, pmmlInterval]
    maximums = [None, 1]
    minimums = [None, 1]
    (extras, children) = pmmlSequence.formatChildren(types, maximums, children)
    if extras != []:
      raise pmmlError, "DiscretizeBin:  " + pmmlErrorStrings.elements
    myChild = pmmlSequence(types, minimums, maximums, children)
    pmmlElement.__init__(self, "DiscretizeBin", myChild, ["binValue"], attributes, ["binValue"])
  
  def __map(self, value):
    """
     Return bin Value if: on boundary and boundary included or
                        within right and left bounds (no boundary
                        being equivalent to +- infinity on that side).
                      else: 
                        return None
    """
    right = self.right
    left = self.left
    # either unbounded on right or less than the boundary:
    matchRight  = ((right is None) or ((right is not None) and (value < right)))
    # either unbounded on left or greater than the boundary:
    matchLeft   = ((left is None) or ((left is not None) and (value > left)))
    # on righthand boundary and it is to be included
    equalsRight = (self.includeRight and value == right)
    # on lefthand boundary and it is to be included.
    equalsLeft  = (self.includeLeft and value == left)
    if (matchRight and matchLeft) or (equalsRight or equalsLeft):
      return self.result
    else:
      return None
 
  def function(self):
    return self.__map
  
  def initialize(self):
    """"""
    child             = self.getChildrenOfType(pmmlInterval)[0]
    self.left         = child.left()
    self.right        = child.right()
    self.includeLeft  = child.includeLeft()
    self.includeRight = child.includeRight()
    self.result       = self.getAttribute("binValue")

def formatObjectIdNoNegatives(num):
  if num >= 0:
    return str(num)
  return "_" + str(abs(num))

class pmmlDiscretize(pmmlElement):
  """Discretize element
  used to normalize a field by using DiscretizeBin elements"""
  def __init__(self, name="", attributes={}, children=[]):
    """define and instantiate the element"""
    types = [pmmlExtension, pmmlDiscretizeBin]
    maximums = [None, None]
    minimums = [None, 1]
    (extras, children) = pmmlSequence.formatChildren(types, maximums, children)
    if extras != []:
      raise pmmlError, "Discretize:  " + pmmlErrorStrings.elements
    myChild = pmmlSequence(types, minimums,  maximums, children)
    pmmlElement.__init__(self, "Discretize", myChild, ["field","mapMissingTo","defaultValue","dataType"], attributes, ["field"])
  
  def dependencies(self):
    return [self.getAttribute("field")]
  
  def code(self, name, indent=""):
    """
    Return code used to evaluate this transformation. Will act like this::

        name = get(self.field)
        if not name is NULL:
          if name is None:
            name = NONE
          else:
            name =  self.transform(get,call)

    """
    self.initialize()
    c = indent + name + " = get('" + self.getAttribute("field") + "')" + """
""" + indent + """if not """ + name + """ is NULL:
""" + indent + """  if """ + name + """ is None:
""" + indent + """    """ + name + """ = """ + str(self.getAttribute("mapMissingTo")) + """
""" + indent + """  else:
""" + indent + """    """ + name + """ =  self.transform(get,call) """ + """
"""
    return c

  def initialize(self):
    """"""
    bins = self.getChildrenOfType(pmmlDiscretizeBin)
    for bin in bins:
       bin.initialize()
    self.bins = [bin.function() for bin in bins]
    attrs = self.getAttributes()
    self.missing = pmmlString.convert(attrs["mapMissingTo"], attrs["dataType"])
    self.default = pmmlString.convert(attrs["defaultValue"], attrs["dataType"])
    self.field = self.getAttribute("field")
  
  def transform(self, get, call):
    """"""
    value = get(self.field)
    if value is NULL:
      return value
    if value is None:
      return self.missing
    else:
      temp = None
      for bin in self.bins:
        temp = bin(value)
        if temp is not None:
          break
      if temp == None:
        return self.default
      else:
        if not self.getAttribute("dataType"):
          return pmmlString.convert(temp, "string")
        else:
          return pmmlString.convert(temp, attrs["dataType"])

class pmmlFieldColumnPair(pmmlElement):
  """FieldColumnPair element
  used to associate a field in the model to a column in a table"""
  def __init__(self, name="", attributes={}, children=[]):
    """define and instantiate the element"""
    myChild = pmmlList([pmmlExtension], children)
    pmmlElement.__init__(self, "FieldColumnPair", myChild, ["field","column"], attributes, ["field","column"])
  
  def dependencies(self):
    return [self.getAttribute("field")]

class pmmlMapValues(pmmlElement):
  """MapValues element
  used to create a field based off of multiple field inputs"""
  def __init__(self, name="", attributes={}, children=[]):
    """define and instantiate the element"""
    types = [pmmlExtension, pmmlFieldColumnPair, choiceTI]
    maximums = [None, None, 1]
    minimums = [None, None, None]
    (extras, children) = pmmlSequence.formatChildren(types, maximums, children)
    if extras != []:
      raise pmmlError, "MapValues:  " + pmmlErrorStrings.elements
    myChild = pmmlSequence(types, minimums, maximums, children)
    pmmlElement.__init__(self, "MapValues", myChild, ["mapMissingTo","defaultValue","outputColumn","dataType"], attributes, ["outputColumn"])
  
  def dependencies(self):
    fields = []
    for child in self.getChildrenOfType(pmmlFieldColumnPair):
      fields.extend(child.dependencies())
    return fields
  
  def code(self, name, indent=""):
    table = self.getChildrenOfType(choiceTI)
    if table:
      table = table[0]
      if isinstance(table, pmmlInlineTable):
        columns = []
        self.fields = []
        for pair in self.getChildrenOfType(pmmlFieldColumnPair):
          columns.append(pair.getAttribute("column"))
          self.fields.append(pair.getAttribute("field"))
        dataType = self.getAttribute("dataType")
        self.matches = table.makeMap(columns, self.getAttribute("outputColumn"), dataType)
        self.missing = self.getAttribute("mapMissingTo")
        if not self.missing is None:
          self.missing = pmmlString.convert(missing, dataType)
        self.default = self.getAttribute("defaultValue")
        if not self.default is None:
          self.default = pmmlString.convert(self.default, dataType)
        return indent + name + """ = self.transform(get, call)
"""
    raise pmmlError, "Map Values requires a usable inline table"
  
  def transform(self, get, call):
    """"""
    values = tuple([str(get(field)) for field in self.fields])
    if NULL in values:
      return NULL
    if values in self.matches:
      return self.matches[values]
    elif None in values:
      return self.missing
    else:
      return self.default

class pmmlAggregate(pmmlElement):
  """Aggregate element
  used to collect information about the counts"""
  def __init__(self, name="", attributes={}, children=[]):
    """define and instantiate the element"""
    myChild = pmmlList([pmmlExtension], children)
    pmmlElement.__init__(self, "Aggregate", myChild, ["field","function","groupField","sqlWhere"], attributes, ["field","function"])
  
  def dependencies(self):
    fields = [self.getAttribute("field")]
    group = self.getAttribute("groupField")
    if group:
      fields.append(group)
    return fields
  
  def __funcCode(self, indent, name, first, second=None):
    """  
         the function acts on a vector (execColumn) which
         is contained within an EventCount instance.

         sum returns a sum of the values.
         multiset returns the list of values
         etc.

         __funcCode("", "new", "test1", None) looks like this:


         if function='sum',
         ------------------
         if isinstance(test1, list):
            new = sum(test1)
         else:
            new = test1.sum()
        
         if function='multiset':
         --------------------
         if not isinstance(test1, list):
          new = test1.multiset()
         else:
          new = test1

    """
    func = self.getAttribute("function")
    if func == "sum":
      if second is None:
        return indent + "if isinstance(" + first + """, list):
""" + indent + "  " + name + " = sum(" + first + """)
""" + indent + """else:
""" + indent + "  " + name + " = " + first + """.sum()
"""
      else:
        return indent + name + " = [" + first + "[cnt] + " + second + "[cnt] for cnt in range(len(" + first + """))]
"""
    elif func == "count":
      if second is None:
        return indent + "if isinstance(" + first + """, list):
""" + indent + "  " + name + " = len(" + first + """)
""" + indent + """else:
""" + indent + "  " + name + " = " + first + """.count()
"""
    elif func == "average":
      if second is None:
        return indent + "if isinstance(" + first + """, list):
""" + indent + """  try:
""" + indent + "    " + name + " = sum(" + first + ") / len(" + first + """)
""" + indent + """  except:
""" + indent + "    " + name + """ = None
""" + indent + """else:
""" + indent + """  try:
""" + indent + "    " + name + " = " + first + """.average()
""" + indent + """  except:
""" + indent + "    " + name + """ = None
"""
      else:
        return indent + name + " = [float(" + first + "[cnt]) / " + second + "[cnt] for cnt in range(len(" + first + """))]
"""
    elif func == "multiset":
      if second is None:
        return indent + "if not isinstance(" + first + """, list):
""" + indent + "  " + name + " = " + first + """.multiset()
""" + indent + """else:
""" + indent + "  " + name + " = " + first + """
"""
    elif func == "keys":
      if second is None:
        return indent + name + " = " + first + """.keys()
"""
    elif func == "min":
      if second is None:
        return indent + "if isinstance(" + first + """, list):
""" + indent + "  " + name + " = min(" + first + """)
""" + indent + """else:
""" + indent + "  " + name + " = " + first + """.minimum()
"""
    elif func == "max":
      if second is None:
        return indent + "if isinstance(" + first + """, list):
""" + indent + "  " + name + " = max(" + first + """)
""" + indent + """else:
""" + indent + "  " + name + " = " + first + """.maximum()
"""
    return None
  
  def code(self, name, indent=""):
    myID = "aggregate" + str(formatObjectIdNoNegatives(id(self)))
    first = myID + "first"
    #check to ensure that first is not missing
    out = indent + first + " = get('" + self.getAttribute("field") + "')" + """
""" + indent + "if not " + first + " is None and not " + first + """ is NULL:
"""
    indent += "  "
    groupField = self.getAttribute("groupField")
    if groupField:
      #check to ensure that second is not missing
      second = myID + "second"
      out += indent + second + " = get('" + groupField + """')
""" + indent + "if not " + second + " is None and not " + second + """ is NULL:
"""
      indent += "  "
      #append the correct functionality
      out += self.__funcCode(indent, name, first, second)
    else:
      #append the correct functionality
      out += self.__funcCode(indent, name, first)
    return out

class choiceCF(pmmlChoice):
  """"""
  # pmmlApply is appended to the given types after it is defined below
  types = [pmmlConstant, pmmlFieldRef, pmmlNormContinuous, pmmlNormDiscrete, pmmlDiscretize, pmmlMapValues, pmmlAggregate]
  
  @staticmethod
  def add(aType):
    """Adds a type to the choiceCF.types list.  Should only be used for the pmmlApply selfreferential loop."""
    choiceCF.types.append(aType)
  
  __maximums = [1, 1, 1, 1, 1, 1, 1, 1]
  def __init__(self, instances):
    """"""
    minimums = [1, 1, 1, 1, 1, 1, 1, 1]
    
    pmmlChoice.__init__(self, choiceCF.types, minimums, choiceCF.__maximums, instances)
    
  @staticmethod
  def formatChildren(children):
    """"""
    return pmmlChoice.formatChildren(choiceCF.types, choiceCF.__maximums, children)

class pmmlApply(pmmlElement):
  """Apply element
  used to define a function call using a set of parameters"""
  def __init__(self, name="", attributes={}, children=[]):
    """define and instantiate the element"""
    types = [pmmlExtension, choiceCF]
    maximums = [None, None]
    minimums = [None, None]
    (extras, children) = pmmlSequence.formatChildren(types, maximums, children)
    if extras != []:
      raise pmmlError, "Apply:  " + pmmlErrorStrings.elements
    myChild = pmmlSequence(types, minimums, maximums, children)
    pmmlElement.__init__(self, "Apply", myChild, ["function"], attributes, ["function"])
  
  def initialize(self):
    """"""
    self.function = self.getAttribute("function")
    self.parameters = self.getChildrenOfType(choiceCF)
    for parameter in self.parameters:
      parameter.initialize()
  
  def code(self, name, indent=""):
    out = ""
    parameters = []
    ID = "Apply" + str(formatObjectIdNoNegatives(id(self)))
    for parameter in self.getChildrenOfType(choiceCF):
      tempName = ID + "Parameter" + str(formatObjectIdNoNegatives(id(parameter)))
      out += parameter.code(tempName, indent)
      parameters.append(tempName)
    codeParameters =  ID + "Parameters"
    #out += indent + codeParameters + """ = [""" + ", ".join(parameters) + """]
#""" + indent + "if NULL in " + codeParameters + """:
#""" + indent + "  " + name + """ = NULL
#""" + indent + "elif None in " + codeParameters + """:
#""" + indent + "  " + name + """ = None
#""" + indent + """else:
#""" + indent + "  " + name + " = call('" + #self.getAttribute("function") + "', " + codeParameters + """)
#"""
    out += indent + codeParameters + """ = [""" + ", ".join(parameters) + """]
""" + indent + """try:
""" + indent + "  " + name + " = call('" + self.getAttribute("function") + "', " + codeParameters + """)
""" + indent + """except:
""" + indent + "  " + name + """ = None
"""
    return out
  
  def dependencies(self):
    fields = []
    for child in self.getChildrenOfType(choiceCF):
      fields.extend(child.dependencies())
    return fields

choiceCF.add(pmmlApply)

class pmmlParameterField(pmmlEmptyElement):
  """ParameterField element
  used to represent a parameter in a new function"""
  def __init__(self, name="", attributes={}, children=[]):
    """define and instantiate the element"""
    if children != []:
      raise pmmlError, "ParameterField:  " + pmmlErrorStrings.noElements
    pmmlElement.__init__(self, "ParameterField", myChild, ["name","optype","dataType"], attributes,["name"])

class pmmlDerivedField(pmmlElement):
  """DerivedField element
  used to define a field derived from an expression"""
  def __init__(self, name="", attributes={}, children=[]):
    """define and instantiate the element"""
    types = [pmmlExtension, choiceCF, pmmlValue]
    maximums = [None, 1, None]
    minimums = [None, 1, None]
    (extras, children) = pmmlSequence.formatChildren(types, maximums, children)
    if extras != []:
      raise pmmlError, "DerivedField:  " + pmmlErrorStrings.elements
    myChild = pmmlSequence(types, minimums, maximums, children)
    pmmlElement.__init__(self, "DerivedField", myChild, ["name", "displayName","optype","dataType"], attributes, ["optype", "dataType"])
  
  def code(self, name):
    c = self.getChildrenOfType(choiceCF)[0].code(name)
    dataType = self.getAttribute("dataType")
    c += """if %s is not None:
  if %s is not NULL:
    if not isinstance(%s, list):
      %s = pmmlString.convert(%s, '%s')
""" % (name, name, name, name, name, dataType)
    return c
  
  def column(self, get, call):
    name = self.getAttribute("name")
    return execColumn(name, self.code(name), "DerivedField:  " + name, localDict={"self":self.getChildrenOfType(choiceCF)[0], "pmmlString":pmmlString, "NULL":NULL, "pmmlErrorStrings":pmmlErrorStrings, "pmmlError":pmmlError, "get":get, "call":call, "operator":operator})
  
  def dependencies(self):
    fields = []
    for child in self.getChildrenOfType(choiceCF):
      fields.extend(child.dependencies())
    return fields

class vector(execColumn):
  """utility class for an EventCount object"""
  def __init__(self, name, get, segmentField=None, countField=None):
    """"""
    self.__name = name
    self.__get = get
    if segmentField:
      if countField:
        self.__countField = countField
        self.__call__ = self.__fieldUpdate
      self.__segmentField = segmentField
      self.__counts = {}
    else:
      pass
  
  def __fieldUpdate(self):
    """"""
    self.__remove = None
    value = self.__get(self.__segmentField)
    num = self.__get(self.__countField)
    if not value is NULL and not num is NULL and not num is None:
      if value in self.__counts:
        self.__counts[value] += num
      else:
        self.__counts[value] = long(num)
      self.__remove = (value, num)
  
  def segmentField(self):
    return self.__segmentField

  def value(self):
    return self
  
  def name(self):
    return self.__name
  
  def __call__(self):
    """"""
    self.__remove = None
    value = self.__get(self.__segmentField)
    if not value is NULL:
      if value in self.__counts:
        self.__counts[value] += 1
      else:
        self.__counts[value] = long(1)
      self.__remove = (value, 1)
  
  def revert(self):
    """"""
    if self.__remove:
      num = self.__remove[1]
      value = self.__remove[0]
      if self.__counts[value] == num:
        del self.__counts[value]
      else:
        self.__counts[value] -= num
  
  def mapping(self):
    """"""
    return self.__counts
  
  def keys(self):
    """"""
    return self.__counts.keys()
  
  def values(self):
    """"""
    return self.__counts.values()
  
  def sum(self):
    """"""
    if not self.__counts:
      return 0
    return sum(self.__counts.values())
  
  def count(self):
    """"""
    return len(self.__counts)
  
  def average(self):
    """"""
    if not self.__counts:
      return 0
    return float(self.sum())/self.count()
  
  def minimum(self):
    """"""
    if not self.__counts:
      return 0
    return min(self.values())
  
  def maximum(self):
    """"""
    if not self.__counts:
      return 0
    return max(self.values())
  
  def multiset(self):
    """"""
    return self.values()


class pmmlEventCount(pmmlElement):
  """"""
  def __init__(self, name="", attributes={}, children=[]):
    """define and instantiate the element"""
    myChild = pmmlList([pmmlExtension], children)
    pmmlElement.__init__(self, "EventCount", myChild, ["name", "segmentField", "countField"], attributes, ["name"])
  
  def column(self, get, call):
    """"""
    return vector(self.getAttribute("name"), get, self.getAttribute("segmentField"), self.getAttribute("countField"))
  
  def dependencies(self):
    """"""
    dep = []
    field = self.getAttribute("segmentField")
    if field:
      dep.append(field)
    field = self.getAttribute("countField")
    if field:
      dep.append(field)
    return dep

def pmmlStandardFunctionThreshold(x, y):
  """"""
  if x>y:
    return 1
  else:
    return 0

def dateDaysSinceYear(aList):
  """
  aList[1] is a year (integer).
  aList[0] is a date or datetime.
  """
  if isinstance(aList[0],DT.datetime):
    return (aList[0]-DT.datetime(aList[1],1,1)).days+1
  elif isinstance(aList[0],DT.date):
    return (aList[0]-DT.date(aList[1],1,1)).days+1
  else:
    raise Exception('Input to dateDaysSinceYear must be a date or datetime!')

pmmlStandardFunctions = {
  "+":lambda aList:aList[0] + aList[1],
  "-":lambda aList:aList[0] - aList[1],
  "*":lambda aList:aList[0] * aList[1],
  "/":lambda aList:aList[0] / aList[1],
  "%":lambda aList:aList[0] % aList[1],
  "min":min,
  "max":max,
  "sum":lambda aList:reduce(operator.add, aList),
  "avg":lambda aList:numpy.mean(aList),
  "log10":lambda aList:numpy.log10(aList[0]),
  "ln":lambda aList:numpy.log(aList[0]),
  "sqrt":lambda aList:numpy.sqrt(aList[0]),
  "abs":lambda aList:numpy.abs(aList[0]),
  "exp":lambda aList:numpy.exp(aList[0]),
  "pow":lambda aList:numpy.power(aList[0], aList[1]),
  "threshold":lambda aList:pmmlStandardFunctionThreshold(aList[0], aList[1]),
  "floor":lambda aList:numpy.floor(aList[0]),
  "ceil":lambda aList:numpy.ceil(aList[0]),
  "round":lambda aList:numpy.round(aList[0]),
  "isMissing":lambda aList:aList[0] is None ,
  "isNotMissing":lambda aList:aList[0] is not None ,
  "equal":lambda aList:aList[0] == aList[1] ,
  "notEqual":lambda aList:aList[0] != aList[1] ,
  "lessThan":lambda aList:aList[0] < aList[1] ,
  "lessOrEqual":lambda aList:aList[0] <= aList[1] ,
  "greaterThan":lambda aList:aList[0] > aList[1] ,
  "greaterOrEqual":lambda aList:aList[0] >= aList[1] ,
  "and":all ,
  "or":any ,
  "not":lambda aList:not aList[0] ,
  "isIn":lambda aList:aList[0] in aList[1:] ,
  "isNotIn":lambda aList:aList[0] in aList[1:] ,
  "if":lambda aList: aList[1] if aList[0] else aList[2],
  "uppercase":lambda aList:aList[0].upper(),
  "substring":lambda aList:aList[0][aList[1]-1:aList[1] + aList[2] - 1],
  "trimBlanks":lambda aList:aList[0].strip(),
  "formatNumber":lambda aList:aList[1] % aList[0],
  "dateDaysSinceYear":dateDaysSinceYear,
  "dateSecondsSinceYear":lambda aList:pmmlStandardFunctions['dateDaysSinceYear'](aList)*86400 + (aList[0]-DT.datetime(aList[1],1,1)).seconds,
  "dateSecondsSinceMidnight":lambda aList:(aList[0]-aList[0].replace(hour=0, minute=0, second=0)).seconds,
  }

class pmmlLocalTransformations(pmmlElement):
  """LocalTransformations element
  used to define transformations utilizing a local namespace for each TestDistribution element"""
  def __init__(self, name="", attributes={}, children=[]):
    """define and instantiate the element"""
    types = [pmmlExtension, pmmlEventCount, pmmlDerivedField]
    maximums = [None, None, None]
    minimums = [None, None, None]
    if attributes != {}:
      raise pmmlError, "LocalTransformations:  " + pmmlErrorStrings.noAttributes
    (extras, children) = pmmlSequence.formatChildren(types, maximums, children)
    if extras != []:
      raise pmmlError, "LocalTransformations:  " + pmmlErrorStrings.elements
    myChild = pmmlSequence(types, minimums, maximums, children)
    pmmlElement.__init__(self, "LocalTransformations", myChild)
    self.__functions = pmmlStandardFunctions

  def initialize(self):
    self.__fields = self.getChildrenOfType(pmmlEventCount)
    self.__fields.extend(self.getChildrenOfType(pmmlDerivedField))
    
  def dictionary(self, get):
    return dictionary(self.__fields, get, self.__call, True)
  
  def aggregates(self):
    """Return the fields that are aggregations."""
    aggregates = []
    for field in self.getChildrenOfType(pmmlDerivedField):
      child = field.getChildrenOfType(choiceCF)[0]
      if isinstance(child, pmmlAggregate):
        aggregates.append(field.getAttribute("name"))
    return aggregates
  
  def nonAggregates(self):
    """Return the fields that are not aggregations."""
    nonAggregates = [count.getAttribute("name") for count in self.getChildrenOfType(pmmlEventCount)]
    for field in self.getChildrenOfType(pmmlDerivedField):
      child = field.getChildrenOfType(choiceCF)[0]
      if not isinstance(child, pmmlAggregate):
        nonAggregates.append(field.getAttribute("name"))
    return nonAggregates
  
  def __call(self, name, parameters=[]):
    """"""
    if name in self.__functions:
      return self.__functions[name](parameters)
    else:
      raise pmmlError, "LocalTransformations:  " + pmmlErrorStrings.notDefined + name

class pmmlDefineFunction(pmmlElement):
  """"""
  def __init__(self, name="", attributes={}, children=[]):
    """"""
    types = [pmmlExtension, pmmlParameterField, choiceCF]
    maximums = [None, None, 1]
    minimums = [None, 1, 1]
    (extras, children) = pmmlSequence.formatChildren(types, maximums, children)
    if extras != []:
      raise pmmlError, "DefineFunction:  " + pmmlErrorStrings.elements
    myChild = pmmlSequence(types, minimums, maximums, children)
    pmmlElement.__init__(self, "DefineFunction", myChild, ["name","optype","dataType"], attributes, ["name","optype"])

class pmmlTransformationDictionary(pmmlLocalTransformations):
  """TransformationDictionary element
  used to define new functions and a global namespace of derived fields"""
  def __init__(self, name="", attributes={}, children=[]):
    """define and instantiate the element"""
    types = [pmmlExtension, pmmlDefineFunction, pmmlEventCount, pmmlDerivedField]
    maximums = [None, None, None, None]
    minimums = [None, None, None, None]
    if attributes != {}:
      raise pmmlError, "TransformationDictionary:  " + pmmlErrorStrings.noAttributes
    (extras, children) = pmmlSequence.formatChildren(types, maximums, children)
    if extras != []:
      raise pmmlError, "TransformationDictionary:  " + pmmlErrorStrings.elements
    myChild = pmmlSequence(types, minimums, maximums, children)
    pmmlElement.__init__(self, "TransformationDictionary", myChild)
    self.__functions = dict(pmmlStandardFunctions)
  
  def __call(self, name, parameters=[]):
    """"""
    if name in self.__functions:
      return self.__functions[name](parameters)
    else:
      raise pmmlError, "TransformationDictionary:  " + pmmlErrorStrings.notDefined + name
  
  def columns(self, get):
    fields = self.getChildrenOfType(pmmlEventCount)
    fields.extend(self.getChildrenOfType(pmmlDerivedField))
    return [field.column(get, self.__call) for field in fields]
  
  def datatypes(self):
    datatypes = {}
    for field in self.getChildrenOfType(pmmlEventCount) + self.getChildrenOfType(pmmlDerivedField):
      name = field.getAttribute("name")
      datatypes[name] = field.getAttribute("dataType")
    return datatypes
  
  def dependencies(self):
    dep = {}
    for field in self.getChildrenOfType(pmmlDerivedField):
      dep[field.getAttribute("name")] = list(set(field.dependencies()))
    for child in self.getChildrenOfType(pmmlEventCount):
      dep[child.getAttribute("name")] = list(set(child.dependencies()))
    return dep
