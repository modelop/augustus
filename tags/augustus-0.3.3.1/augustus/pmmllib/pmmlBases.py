"""These classes are created to support a generic pmml superstructure.
"""

__copyright__ = """
Copyright (C) 2006-2009  Open Data ("Open Data" refers to
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


def xmlEscape(string):
  """a function which escapes the five standard xml escaped characters:
  &  ->  &amp;
  '  ->  &apos;
  "  ->  &quot;
  >  ->  &gt;
  <  ->  &lt;"""
  for x in range(32):
    string = string.replace(chr(x), "%" + str(x))
  string = string.replace(chr(127), "%" + str(127))
  return string.replace("&", "&amp;").replace("'", "&apos;").replace('"', "&quot;").replace(">", "&gt;").replace("<", "&lt;")

def xmlUnEscape(string):
  """a function which unescapes the five standard xml escaped characters:
  &amp;  ->  &
  &apos; ->  '
  &quot; ->  "
  &gt;   ->  >
  &lt;   ->  <"""
  for x in range(32):
    string = string.replace("%" + str(x), chr(x))
  string = string.replace("%" + str(127), chr(127))
  return string.replace("&amp;", "&").replace("&apos;", "'").replace("&quot;", '"').replace("&gt;", ">").replace("&lt;", "<")

class pmmlError(StandardError):
  """used to indicate an error with the pmml"""
  pass

########################################################################
#Data type conversion constructors
import datetime as DT
def makeDatetime(time):
  """retuns a datetime object given a string like:
    "YYYY*MO*DA*HO*MI*SE"
  where:
    * is any character
    year(YYYY), month(MO), day(DA), hour(HO), minute(MI), and second(SE)
      are in the ranges defined by datetime.
    Tries to sensibly pad things out if higher precision pieces are missing.
  """
  intime = len(time)
  try:
    sep = time[4]
  except IndexError:
    # Assume we just have a year
    return DT.datetime(int(time),1,1)
  if (intime < 19):
    # default day, if needed, is '01'
    defday = ((19 - intime) / 12) * ((sep + '01'))
    # default hour,minute,seconds are 00
    defhourminuteseconds = ((19 - intime)/3) * ((sep + '00'))
    time = time + defday + defhourminuteseconds 
  return DT.datetime(int(time[0:4]), int(time[5:7]), int(time[8:10]), int(time[11:13]), int(time[14:16]), int(time[17:19]))

import time
def makeSecondsSince(when, year):
  """returns a datetime object given an integer number of seconds and a
  year from which the seconds are counted"""
  timestamp = time.localtime(int(when) + time.mktime((year,1,1,0,0,0,0,1,-1)))
  return DT.datetime(timestamp[0], timestamp[1], timestamp[2], timestamp[3], timestamp[4], timestamp[5])

########################################################################
#string objects
class pmmlString(str):
  """any valid string"""
  @staticmethod
  def convert(value, dataType):
    """returns the value typecast to the correct data type
    raises a TypeError if the given data type is not supported"""
    if dataType is None:
      return value
    if dataType == "integer":
      return long(value)
    elif dataType == "float":
      return float(value)
    elif dataType == "double":
      return float(value)
    elif dataType == "dateTime":
      return makeDatetime(value)
    elif dataType == "string":
      return str(value)
    elif dataType[:20] == "dateTimeSecondsSince":
      return makeSecondsSince(value, int(dataType[21:][:-1]))
    raise TypeError, "Given type not supported:  " + str(dataType)
  
  def __str__(self, indentation="", step="", spacing=""):
    """the extra arguments are used to allow the string to be
      output in an easily legible xml format
    returns a string as follows:
    >>> x = pmmlString("15")
    >>> print x
    15
    >>> print x.__str__("_","-","^")
    _15^"""
    return indentation + self + spacing

#############################################################################
#container classes
#
#Choice and Sequence may hold instances of other containers within themselves;
#however, the other containers may not hold instances of containers.
class pmmlContainer:
  """superclass for containing pmml classes"""
  def __init__(self, types):
    """stores which types are to be contained in this class
    raises an error if any given type is not a subclass of the following:
      pmmlElement, pmmlString, pmmlContainer"""
    for aType in types:
      if not issubclass(aType, pmmlTypes):
        raise pmmlError, "Non-pmml types are not allowed in containers"
    self.__types = tuple(types)
  
  def getTypes(self):
    """returns which types are allowed in this container"""
    return self.__types
  
  def validateInstance(self, instance):
    """raises an error if the instance is not of a type allowed in this container"""
    if not isinstance(instance, self.__types):
      raise pmmlError, "Given object type not allowed in this container"

class pmmlList(pmmlContainer):
  """contains pmml objects in order"""
  def __init__(self, types, instances=[]):
    """records which types are allowed in this container and then attempts to add each instance"""
    pmmlContainer.__init__(self, types)
    
    self.__list = []
    for instance in instances:
      pmmlList.addInstance(self, instance)
  
  def getList(self):
    """returns a list of all instances in the order they were added"""
    return self.__list
  
  def getLength(self):
    """returns the number of instances"""
    return len(self.__list)
  
  def addInstance(self, instance):
    """validates then adds the instance"""
    self.validateInstance(instance)
    self.__list.append(instance)
  
  def removeInstance(self, instance):
    """removes an instance if it is in the list"""
    if instance in self.__list:
      self.__list.remove(instance)
  
  def replaceInstance(self, oldInstance, newInstance):
    if oldInstance in self.__list:
      ind = self.__list.index(oldInstance)
      self.__list.remove(oldInstance)
      self.__list.insert(ind, newInstance)

  def getChildrenOfType(self, aType):
    """if the given type is a subclass of pmmlElement:
      return all instances of the given type in the instances
    else:
      return all instances of any instance of the given type"""
    children = []
    for entry in self.getList():
      if isinstance(entry, aType):
        if isinstance(entry, pmmlElement):
          children.append(entry)
        else:
          children.extend(entry.getChildren())
      elif isinstance(entry, pmmlList):
        children.extend(entry.getChildrenOfType(aType))
    return children
  
  def pop(self):
    """removes the last instance added"""
    self.__list.pop()
  
  def __str__(self, indentation="", step="", spacing=""):
    """the extra arguments are used to allow the string to be
      output in an easily legible xml format
    returns a string as follows:
    >>> x = pmmlList([pmmlString], [pmmlString("5"),pmmlString("1")])
    >>> print x
    51
    >>> print x.__str__("_","-","^")
    _5^_1^"""
    return "".join([instance.__str__(indentation, step, spacing) for instance in self.getList()])
  
  def getChildren(self):
    """returns all instances of pmmlElement or pmmlString stored here or in the instances"""
    children = []
    for child in self.getList():
      if isinstance(child, pmmlList):
        children.extend(child.getChildren())
      else:
        children.append(child)
    return children

class pmmlBoundedList(pmmlList):
  """limits the minimum and maximum number of contents
    if minimum or maximum is None, that boundary is considered non-existant"""
  def __init__(self, types, minimum, maximum, instances=[]):
    """validates and adds all instances to the list
    asserts that the length is above the minimum and below the maximum"""
    pmmlList.__init__(self, types, instances)
    
    self.__minimum = minimum
    self.__maximum = maximum
    if not self.__minimum is None and self.getLength() < self.__minimum:
      raise pmmlError, "List initialized with too few elements"
    if not self.__maximum is None and self.getLength() > self.__maximum:
      raise pmmlError, "List initialized with too many elements"
  
  def getMinimum(self):
    """returns the minimum number of instances allowed"""
    return self.__minimum
  
  def getMaximum(self):
    """returns the maximum number of instances allowed"""
    return self.__maximum
  
  def validateAdd(self):
    """raises an error if the length is equal to the maximum"""
    if not self.__maximum is None and self.getLength() == self.__maximum:
      raise pmmlError, "List contains maximum number of objects:  can not add"
  
  def validateRemove(self):
    """raises an error if the length is equal to the minimum"""
    if not self.__minimum is None and self.getLength() == self.__minimum:
      raise pmmlError, "List contains minimum number of objects:  can not remove"
  
  def addInstance(self, instance):
    """validates and adds an instance"""
    self.validateAdd()
    pmmlList.addInstance(self, instance)
  
  def removeInstance(self, instance):
    """validates and removes an instance"""
    self.validateRemove()
    pmmlList.removeInstance(self, instance)
  
  def replaceInstance(self, oldInstance, newInstance):
    """replace a child with a new one"""
    pmmlList.replaceInstance(self, oldInstance, newInstance)

  def pop(self):
    """validates and removes the last instance added"""
    self.validateRemove()
    pmmlList.pop(self)

class pmmlStrictList(pmmlBoundedList):
  """further requires that the list contain only one type of content"""
  def __init__(self, aType, instances=[], minimum=None, maximum=None):
    """calls pmmlBoundedList.__init__ with the given options and [aType]"""
    pmmlBoundedList.__init__(self, [aType], minimum, maximum, instances)
  
  def getType(self):
    """returns the type of the instances"""
    return self.getTypes()[0]

class pmmlChoice(pmmlStrictList):
  """contains only one type of instance"""
  @staticmethod
  def formatChildren(types, maximums, children=[]):
    """forms a instance list from the given children using the first type that applies
    returns a list of children that didn't fit and the instance list formed
    like:
    >>> #instances all fit
    >>> pmmlChoice.formatChildren([pmmlString], [2], [pmmlString("5")])
    ([], [pmmlString("5")])
    >>> #since there are too many pmmlString elements, the entire list (except what has already been processed) is leftover
    >>> pmmlChoice.formatChildren([pmmlString], [2], [pmmlString("5"), pmmlString("6"), pmmlString("7"), pmmlString("8")])
    ([pmmlString("7"), pmmlString("8")], [pmmlString("5"), pmmlString("6")])
    >>> #since an object of the wrong type is passed, the entire list (except what has already been processed) is leftover
    >>> pmmlChoice.formatChildren([pmmlString], [2], [pmmlString("5"), object(), pmmlString("6")])
    ([pmmlAnnotation(), pmmlString("6")], [pmmlString("5")])"""
    leftovers = list(children)
    typeIndex = 0
    instances = []
    while typeIndex < len(types) and len(leftovers) > 0:
      
      curType = types[typeIndex]
      #recurse on child type if it is a choice or sequence subclass
      if issubclass(curType, (pmmlChoice, pmmlSequence)):
        (extras, produced) = curType.formatChildren(children)
        while produced != []:
          instances.append(curType(produced))
          if not maximums[typeIndex] or len(instances) != maximums[typeIndex]:
            (extras, produced) = curType.formatChildren(extras)
          else:
            break
        if instances != []:
          leftovers = extras
      else:
        top = len(leftovers)
        cur = 0
        while cur < top and isinstance(leftovers[cur], curType) and \
              (not maximums[typeIndex] or cur != maximums[typeIndex]):
          cur += 1
        instances.extend(leftovers[:cur])
        leftovers = leftovers[cur:]
      
      if instances != []:
        break
      
      typeIndex += 1
    
    return (leftovers, instances)
  
  def __init__(self, types, minimums, maximums, instances):
    """calls pmmlStrictList.__init__ with the first type that matches the first instance
      and the associated minimum and maximum"""
    self.__types = list(types)
    self.__minimums = list(minimums)
    self.__maximums = list(maximums)
    
    index = 0
    for aType in types:
      if len(instances) > 0 and isinstance(instances[0], aType):
        found = index
        break
      index += 1
    pmmlStrictList.__init__(self, types[found], instances, minimums[found], maximums[found])
  
  def getTypes(self):
    """returns all types possible"""
    return self.__types
  
  def getMinimums(self):
    """returns the minimums"""
    return self.__minimums
  
  def getMaximums(self):
    """returns the maximums"""
    return self.__maximums

class pmmlSequence(pmmlList):
  """contains a single set of strict lists and is ordered by the types given
  care is needed when lists of the same type or subtypes are utilized"""
  @staticmethod
  def formatChildren(types, maximums, children=[]):
    """forms the given children into a set with the given types
    returns a list of children that didn't fit and the set formed
    like:
    >>> #instances all fit
    >>> pmmlSequence.formatChildren([pmmlString, pmmlExtension], [1,1], [pmmlString("5"), pmmlExtension()])
    ([], [[pmmlString("5")], [pmmlExtension()]])
    >>> #since there are too many pmmlString elements, the entire list (except what has already been processed) is leftover
    >>> pmmlSequence.formatChildren([pmmlString], [1], [pmmlString("5"), pmmlString("6"), pmmlString("7")])
    ([pmmlString("6"), pmmlString("7")], [[pmmlString("5")]])
    >>> #since an object of the wrong type is passed, the entire list (except what has already been processed) is leftover
    >>> pmmlSequence.formatChildren([pmmlString], [2], [pmmlString("5"), object(), pmmlString("6")])
    ([pmmlAnnotation(), pmmlString("6")], [[pmmlString("5")]])"""
    leftovers = list(children)
    typeIndex = 0
    newSet = [[]]
    newSetIndex = 0
    while typeIndex < len(types) and len(leftovers) > 0:
      curResults = newSet[newSetIndex]
      curType = types[typeIndex]
      
      #recurse on child type if it is a choice or sequence subclass
      if issubclass(curType, (pmmlChoice, pmmlSequence)):
        extras = leftovers
        (extras, produced) = curType.formatChildren(extras)
        while produced != []:
          curResults.append(curType(produced))
          if not maximums[typeIndex] or len(curResults) != maximums[typeIndex]:
            (extras, produced) = curType.formatChildren(extras)
          else:
            break
        if curResults != []:
          leftovers = extras
      else:
        top = len(leftovers)
        cur = 0
        while cur < top and isinstance(leftovers[cur], curType) and \
              (not maximums[typeIndex] or cur != maximums[typeIndex]):
          cur += 1
        curResults.extend(leftovers[:cur])
        leftovers = leftovers[cur:]
      
      if curResults != []:
        newSetIndex += 1
        newSet.append([])
      
      typeIndex += 1
      
    if newSet[len(newSet) - 1] == []:
      newSet.pop()
     
    return (leftovers, newSet)
  
  def __init__(self, types, minimums, maximums, set=[]):
    """initializes a pmmlStrictList for each type
    for type in types:
      if the next member of the set is of the given type:
        pass the set as the instances
      else:
        initialize an empty list
    where:
      set is like a set returned from pmmlSequence.formatChildren"""
    pmmlList.__init__(self, [pmmlStrictList])
    
    self.__setTypes = list(types)
    self.__minimums = list(minimums)
    self.__maximums = list(maximums)
    
    setIndex = 0
    typeIndex = 0
    for aType in self.__setTypes:
      if setIndex < len(set) and len(set[setIndex]) > 0 and isinstance(set[setIndex][0], aType):
        pmmlList.addInstance(self, pmmlStrictList(aType, set[setIndex], minimums[typeIndex], maximums[typeIndex]))
        setIndex += 1
      else:
        pmmlList.addInstance(self, pmmlStrictList(aType, [], minimums[typeIndex], maximums[typeIndex]))
      typeIndex += 1
      
    #require that the given lists be in order according to the types list
    if setIndex < len(set):
      raise pmmlError, "Lists not in type order given or too many lists given"
  
  def getSetTypes(self):
    """the list of types of strict lists"""
    return self.__setTypes
  
  def getMinimums(self):
    """returns the maximums"""
    return self.__minimums
  
  def getMaximums(self):
    """returns the minimums"""
    return self.__maximums
  
  def getSequence(self):
    """returns a list of the instances contained within all strict lists"""
    sequence = []
    for typeList in self.getList():
      current = []
      for child in typeList.getList():
        current.append(child)
      sequence.append(current)
    return sequence
  
  def addInstance(self, child):
    """adds an instance into the first strict list it matches if it matches"""
    index = 0
    for aType in self.__setTypes:
      if isinstance(child, aType):
        typeLists = self.getList()
        typeLists[index].addInstance(child)
        break
      index += 1
  
  def removeInstance(self, child):
    """removes the first occurance of an instance if it exists"""
    index = 0
    for aType in self.__setTypes:
      if isinstance(child, aType):
        typeLists = self.getList()
        typeLists[index].removeInstance(child)
        break
      index += 1

  def replaceInstance(self, oldChild, newChild):
    """ replace an old instance with a new instance if
        old one exists and new one is valid"""
    index = 0
    for aType in self.__setTypes:
      if isinstance(oldChild, aType):
        typeLists = self.getList()
        typeLists[index].replaceInstance(oldChild, newChild)
        break
      index += 1
  
  def __str__(self, indentation="", step="", spacing=""):
    """the extra arguments are used to allow the string to be
      output in an easily legible xml format
    returns a string as follows:
    >>> x = pmmlSequence([pmmlString, pmmlExtension], [None, None], [None, None], [[pmmlString("5"), pmmlString("1")]])
    >>> print x
    51
    >>> print x.__str__("_","-","^")
    _5^_1^"""
    return "".join([instance.__str__(indentation, step, spacing) for instance in self.getList()])

#############################################################################
#base element classes
class pmmlElement:
  """defines any pmml element"""
  def __init__(self, name, child, attributeNames=[], attributes={}, requiredAttributes=[], whiteSpaceImportant=False):
    """records the name of the element
    stores its child
    initializes attributes (unescapes into characters from this list:  [<,>,',",&])
    checks for required attributes
    stores whether or not white space needs to be preserved within this element"""
    #set name
    self.__name = name
    
    #set attribute names, order, and initial value
    self.__attributes = {}
    self.__attributeNames = []
    for name in attributeNames:
      if name in attributes:
        self.makeAttribute(name, xmlUnEscape(attributes[name]))
      else:
        self.__attributes[name] = None
      self.__attributeNames.append(name)
    #set required attributes
    self.__requiredAttributes = []
    for name in requiredAttributes:
      self.__requiredAttributes.append(name)
      if not name in self.__attributes or self.__attributes[name] is None:
        raise pmmlError, self.__name + ":  Attribute is not in element:  " + str(name)
    
    #add the child
    if not isinstance(child, pmmlTypes):
      raise pmmlError, self.__name + ":  Element child must be a valid pmml type"
    self.__child = child
    
    #check that all the required attributes have been initialized
    for name in self.__requiredAttributes:
      if self.__attributes[name] is None:
        raise pmmlError, self.__name + ":  A required attribute wasn't initialized:  " + str(name)
    
    #store whether or not white space is important for this element
    self.__whiteSpaceImportant = whiteSpaceImportant
  
  def getName(self):
    """returns the name of the element"""
    return self.__name
  
  def getAttributes(self):
    """returns all the attributes of the element"""
    return self.__attributes
  
  def getRequiredAttributes(self):
    """returns all attributes that are required to have values at all times"""
    return self.__requiredAttributes
  
  def getAttribute(self, name):
    """returns the given attribute value or the value None to indicate that
      the given attribute has no value"""
    if self.__attributes.has_key(name):
      return self.__attributes[name]
    else:
      return None
  
  def getChild(self):
    """returns the child object"""
    return self.__child
  
  def getChildren(self):
    """returns all the child elements or strings in a list"""
    child = self.getChild()
    if isinstance(child, pmmlContainer):
      children = child.getChildren()
    else:
      children = [child]
    return children
  
  def getChildrenOfType(self, aType):
    """returns all child elements or strings that are of the given type"""
    child = self.getChild()
    if isinstance(child, pmmlContainer):
      return child.getChildrenOfType(aType)
    elif isinstance(child, aType):
      return [child]
    return []
  
  def addChild(self, child):
    """attempts to add an instance to the child container object
    raises an error if the child is not a container"""
    if isinstance(self.__child, pmmlContainer):
      self.__child.addInstance(child)
    else:
      raise pmmlError, self.__name + ":  Element can not contain another child"
  
  def addChildren(self, children):
    """attempts to add instances to the child container object
    raises an error if the child is not a container"""
    if isinstance(self.__child, pmmlContainer):
      for child in children:
        self.__child.addInstance(child)
    else:
      raise pmmlError, self.__name + ":  Element can not contain other children"
  
  def removeChild(self, child):
    """attempts to remove an instance from the child container object
    raises an error if the child is not a container"""
    if isinstance(self.__child, pmmlContainer):
      self.__child.removeInstance(child)
    else:
      raise pmmlError, self.__name + ":  Element can not remove child"
  
  def replaceChild(self, oldChild, newChild):
    """attempts to remove an instance from the child container object
    raises an error if the child is not a container"""
    if isinstance(self.__child, pmmlContainer):
      self.__child.replaceInstance(oldChild, newChild)
    else:
      raise pmmlError, self.__name + ":  Element can not remove child"

  def removeChildren(self, children):
    """attempts to remove instances from the child container object
    raises an error if the child is not a container"""
    if isinstance(self.__child, pmmlContainer):
      for child in children:
        self.__child.removeInstance(child)
    else:
      raise pmmlError, self.__name + ":  Element can not remove child"
  
  def makeAttribute(self, name, value):
    """sets the value of an attribute"""
    self.__attributes[name] = str(value)
  
  def addAttributes(self, attributes):
    """sets the values of the attributes
    attributes = {"field":"age","race":"fast"} makes:
      <name field="name" race="slow" type="O-"/>
    into:
      <name field="race" race="fast" type="O-"/>"""
    for name in attributes:
      self.makeAttribute(name, attributes[name])
  
  def removeAttribute(self, name):
    """effectively removes the attribute from the element if it isn't 
      required to remain in the element"""
    if name in self.__requiredAttributes:
      raise pmmlError, self.__name + ":  Can not remove required attribute:  " + repr(name)
    else:
      self.__attributes[name] = None
  
  """the following variables allow for a common indentation, step, and spacing scheme
  throughout all pmmlElement instances"""
  __step = "  "
  __spacing = "\n"
  __top = True
  
  def __str__(self, indentation="", step="", spacing=""):
    """the extra arguments are used to allow the string to be
      output in an easily legible xml format
    returns a string as follows:
    >>> x = pmmlElement("a", pmmlString(""), ["href"], {"href":"http://www.com", "m":None}, ["href"])
    >>> print x + "!"
    <a href="http://www.com" />!
    >>> x = pmmlElement("a", pmmlString("hi"), ["href"], {"href":"http://www.com", "m":None}, ["href"])
    >>> print x + "!"
    <a href="http://www.com">
      hi
    </a>!
    >>> x = pmmlElement("a", pmmlString("hi"), ["href"], {"href":"http://www.com"}, ["href"], True)
    >>> print x + "!"
    <a href="http://www.com">hi</a>!
    >>> y = pmmlElement("a", x, ["href"], {"href":"http://com.www"}, ["href"])
    >>> print y + "!"
    <a href="http://com.www">
      <a href="http://www.com">hi</a>!
    </a>!
    >>> z = pmmlElement("a", y, ["href"], {"href":"http://www.net"}, ["href"])
    >>> print z + "!"
    <a href="http://www.net">
      <a href="http://com.www">
        <a href="http://www.com">hi</a>!
      </a>!
    </a>!
    >>> print z.__str__("_","-","^") + "!"
    _<a href="http://www.net">_-<a href="http://com.www">_--<a href="http://www.com">hi</a>!^</a>!^</a>!
    also escapes attribute values:
    >>> x = pmmlElement("a", pmmlString("hi"), ["href"], {"href":"'><&"}, ["href"])
    >>> print x + "!"
    <a href="&apos;&gt;&lt;&amp;" />!
    >>> x = pmmlElement("a", pmmlString("hi"), ["href"], {"href":'"'}, ["href"])
    >>> print x + "!"
    <a href="&quot;" />!"""
    top = False
    if pmmlElement.__top:
      step = pmmlElement.__step
      spacing = pmmlElement.__spacing
      pmmlElement.__top = False
      top = True
    
    out = indentation + "<" + self.__name
    
    for name in self.__attributeNames:
      if self.__attributes[name] or self.__attributes[name]=="":
        out += " " + name + '="' + xmlEscape(self.__attributes[name]) + '"'
    
    #get what the child will print
    if self.__whiteSpaceImportant:
      child = self.__child.__str__("", "", "")
    else:
      child = self.__child.__str__(indentation+step, step, spacing)
    
    if len(child) > 0:
      #close opening tag
      if self.__whiteSpaceImportant:
        out += ">"
      else:
        out += ">" + spacing
      
      #print the child
      out += child
      
      #closing tag
      if self.__whiteSpaceImportant:
        out += "</" + self.__name + ">"
      else:
        out += indentation + "</" + self.__name + ">"
    else:
      #close opening tag
      out += " />"
    
    if top:
      pmmlElement.__top = True
    out += spacing
    
    return out

class pmmlEmptyElement(pmmlElement):
  """represents an element in pmml without any children"""
  def __init__(self, name, attributeNames=[], attributes={}, requiredAttributes=[], whiteSpaceImportant=False):
    """simply passes an empty child for the element"""
    pmmlElement.__init__(self, name, pmmlSequence([], [], []), attributeNames, attributes, requiredAttributes, whiteSpaceImportant)
  
  def getChildren(self):
    """since there are no children, return an empty list"""
    return []
  
  def getChildrenOfType(self, aType):
    """since there are no children, return an empty list"""
    return []
  
  def addChild(self, child):
    """since there are no children, do nothing"""
    pass
  
  def addChildren(self, children):
    """since there are no children, do nothing"""
    pass

"""defines the types allowed in pmmlContainers and as children of pmmlElement objects"""
pmmlTypes = (pmmlElement, pmmlString, pmmlContainer)
