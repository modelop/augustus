""""""


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


from pmmlElements import *

#This file defines PMML elements utilized within models

#for each element, the same functions are defined that are
#described at the top of the pmmlElements.py file.

#############################################################################
#elements that belong to models in general
class pmmlMiningField(pmmlElement):
  """"""
  def __init__(self, name="", attributes={}, children=[]):
    """"""
    myChild = pmmlList([pmmlExtension], children)
    attributeNames = ["name","usageType","optype","importance","outliers",\
    "lowValue","highValue","missingValueReplacement","missingValueTreatment",\
    "invalidValueTreatment"]
    requiredAttributes = ["name"]
    pmmlElement.__init__(self, "MiningField", myChild, attributeNames, attributes, requiredAttributes)

class pmmlMiningSchema(pmmlElement):
  """"""
  def __init__(self, name="", attributes={}, children=[]):
    """"""
    types = [pmmlExtension, pmmlMiningField]
    maximums = [None, None]
    minimums = [None, 1]
    if attributes != {}:
      raise pmmlError, "MiningSchema:  " + pmmlErrorStrings.noAttributes
    (extras, children) = pmmlSequence.formatChildren(types, maximums, children)
    if extras != []:
      raise pmmlError, "MiningSchema:  " + pmmlErrorStrings.elements
    myChild = pmmlSequence(types, minimums, maximums, children)
    pmmlElement.__init__(self, "MiningSchema", myChild)
  
  def getPredicted(self):
    predicted = []
    fields = self.getChildrenOfType(pmmlMiningField)
    for field in fields:
      if (field.getAttribute("usageType")=='predicted'):
        predicted.append(field.getAttribute("name"))
    return predicted
  
  def initialize(self, dataInput, localTrans = None):
    """Initialize the mining schema."""
    self.__datadict = dataInput
    self.__localtrans = localTrans
    
    self.__missingvaluereplacement = {}
    fields = self.getChildrenOfType(pmmlMiningField)
    for field in fields:
      name = field.getAttribute("name")
      mv_string = field.getAttribute("missingValueReplacement")
      if mv_string is None:
        #No missing value replacemnt
        self.__missingvaluereplacement[name] = None
      else:
        #Convert the missing value replacement to the right type
        if name in dataInput:
          self.__missingvaluereplacement[name] = pmmlString.convert(mv_string, dataInput.datatype(name))
        else:
          print "A mining field isn't supposed to refer to a field that's not in the data dictionary."
  
  def get(self, field):
    """Return the value of a field.  Implements missing value replacement."""
    if field in self.__datadict:
      value = self.__datadict[field]
    else:
      try:
        value = self.__localtrans[field]
      except:
        #Anything that makes it here probably means that the model is wrong.
        print "You're trying to access the field \"%s\" but I can't find it.  Please make sure your model is correct." % (field)
        raise
        #value = None
    
    #Check for missing value
    if value is None:
      return self.__missingvaluereplacement.get(field)
    else:
      return value
  
  def td_get(self, field):
    """Return the value of a field.  Implements missing value replacement.
    
    This version is only used for the transformation dictionary because it doesn't know about local transformations."""
    try:
      value = self.__datadict[field]
    except:
      #Anything that makes it here probably means that the model is wrong.
      raise
    
    #Check for missing value
    if value is None:
      return self.__missingvaluereplacement.get(field)
    else:
      return value

class pmmlOutputField(pmmlElement):
  """"""
  def __init__(self, name="", attributes={}, children=[]):
    """"""
    myChild = pmmlList([pmmlExtension], children)
    attributeNames = ["name","displayName","optype","dataType","targetField",\
          "feature","value"]
    requiredAttributes = ["name"]
    pmmlElement.__init__(self, "OutputField", myChild, attributeNames, attributes, requiredAttributes)

class pmmlOutput(pmmlElement):
  """"""
  def __init__(self, name="", attributes={}, children=[]):
    """"""
    types = [pmmlExtension, pmmlOutputField]
    maximums = [None, None]
    minimums = [None, 1]
    if attributes != {}:
      raise pmmlError, "Output:  " + pmmlErrorStrings.noAttributes
    (extras, children) = pmmlSequence.formatChildren(types, maximums, children)
    if extras != []:
      raise pmmlError, "Output:  " + pmmlErrorStrings.elements
    myChild = pmmlSequence(types, minimums, maximums, children)
    pmmlElement.__init__(self, "Output", myChild)

class pmmlCounts(pmmlElement):
  """"""
  def __init__(self, name="", attributes={}, children=[]):
    """"""
    myChild = pmmlList([pmmlExtension], children)
    attributeNames = ["totalFreq","missingFreq","invalidFreq"]
    requiredAttributes = ["totalFreq"]
    pmmlElement.__init__(self, "Counts", myChild, attributeNames, attributes, requiredAttributes)

class pmmlTargetValueCount(pmmlElement):
  """"""
  def __init__(self, name="", attributes={}, children=[]):
    """"""
    types = [pmmlExtension]
    maximums = [None]
    minimums = [0]
    (extras, children) = pmmlSequence.formatChildren(types, maximums, children)
    if extras != []:
      raise pmmlError, "TargetValueCount:  " + pmmlErrorStrings.elements
    myChild = pmmlList([pmmlExtension], children)
    attributeNames = ["value","count"]
    requiredAttributes = ["value","count"]
    pmmlElement.__init__(self, "TargetValueCount", myChild, attributeNames, attributes, requiredAttributes)

class pmmlTargetValueCounts(pmmlElement):
  """"""
  def __init__(self, name="", attributes={}, children=[]):
    """"""
    types = [pmmlExtension, pmmlTargetValueCount]
    maximums = [None, None]
    minimums = [0, 1]
    (extras, children) = pmmlSequence.formatChildren(types, maximums, children)
    if extras != []:
      raise pmmlError, "TargetValueCounts:  " + pmmlErrorStrings.elements
    myChild = pmmlSequence([pmmlExtension,pmmlTargetValueCount], minimums, maximums,children)
    attributeNames = []
    requiredAttributes = []
    pmmlElement.__init__(self, "TargetValueCounts", myChild, attributeNames, attributes, requiredAttributes)

class pmmlPairCounts(pmmlElement):
  """"""
  def __init__(self, name="", attributes={}, children=[]):
    """"""
    types = [pmmlExtension, pmmlTargetValueCounts]
    maximums = [None, 1]
    minimums = [0, 1]
    (extras, children) = pmmlSequence.formatChildren(types, maximums, children)
    if extras != []:
      raise pmmlError, "PairCounts:  " + pmmlErrorStrings.elements
    #myChild = pmmlList([pmmlExtension, pmmlTargetValueCounts], children)
    myChild = pmmlSequence([pmmlExtension,pmmlTargetValueCounts], minimums, maximums,children)
    attributeNames = ["value"]
    requiredAttributes = ["value"]
    pmmlElement.__init__(self, "PairCounts", myChild, attributeNames, attributes, requiredAttributes)


class pmmlQuantile(pmmlElement):
  """"""
  def __init__(self, name="", attributes={}, children=[]):
    """"""
    myChild = pmmlList([pmmlExtension], children)
    attributeNames = ["quantileLimit","quantileValue"]
    requiredAttributes = ["quantileLimit","quantileValue"]
    pmmlElement.__init__(self, "Quantile", myChild, attributeNames, attributes, requiredAttributes)

class pmmlNumericInfo(pmmlElement):
  """"""
  def __init__(self, name="", attributes={}, children=[]):
    """"""
    types = [pmmlExtension, pmmlQuantile]
    maximums = [None, None]
    minimums = [None, None]
    (extras, children) = pmmlSequence.formatChildren(types, maximums, children)
    if extras != []:
      raise pmmlError, "NumericInfo:  " + pmmlErrorStrings.elements
    myChild = pmmlSequence(types, minimums, maximums, children)
    attributeNames = ["minimum","maximum","mean","standardDeviation","median","interQuartileRange"]
    pmmlElement.__init__(self, "NumericInfo", myChild, attributeNames, attributes)

class pmmlArray(pmmlElement):
    """"""
    def __init__(self, name="", attributes={}, children=[]):
        """"""
        try:
            self.array_type = attributes["type"]
        except KeyError:
            raise pmmlError, "Array:  required attribute 'type' not found"

        if self.array_type not in ("int", "real"):
            raise pmmlError, "Array:  'type' must be 'int' or 'real'"

        if self.array_type == "int":
            conversion = int
        else:
            conversion = float

        try:
            self.array_values = map(conversion, filter(lambda x: x != "", re.split(r"\s+", " ".join(children))))
        except ValueError:
            raise pmmlError, "Array:  Array value(s) not correctly formed"

        try:
            self.array_n = int(attributes["n"])
        except (ValueError, KeyError):
            self.array_n = len(self.array_values)

        if len(self.array_values) != self.array_n:
            raise pmmlError, "Array:  Array dimension is not equal to value of attribute 'n'"

    def __str__(self, indentation="", step="", spacing=""):
        return "%s<Array n=\"%d\" type=\"%s\">%s</Array>%s" % (indentation, self.array_n, self.array_type, " ".join(map(str, self.array_values)), spacing)

class pmmlDiscrStats(pmmlElement):
  """"""
  def __init__(self, name="", attributes={}, children=[]):
    """"""
    types = [pmmlExtension, pmmlArray]
    maximums = [None, 2]
    minimums = [None, None]
    (extras, children) = pmmlSequence.formatChildren(types, maximums, children)
    if extras != []:
      raise pmmlError, "DiscrStats:  " + pmmlErrorStrings.elements
    myChild = pmmlSequence(types, minimums, maximums, children)
    attributeNames = ["modalValue"]
    pmmlElement.__init__(self, "DiscrStats", myChild, attributeNames, attributes)

class pmmlContStats(pmmlElement):
  """"""
  def __init__(self, name="", attributes={}, children=[]):
    """"""
    types = [pmmlExtension, pmmlInterval, pmmlArray]
    maximums = [None, None, 3]
    minimums = [None, None, None]
    (extras, children) = pmmlSequence.formatChildren(types, maximums, children)
    if extras != []:
      raise pmmlError, "ContStats:  " + pmmlErrorStrings.elements
    myChild = pmmlSequence(types, minimums, maximums, children)
    attributeNames = ["totalValuesSum","totalSquaresSum"]
    pmmlElement.__init__(self, "ContStats", myChild, attributeNames, attributes)

class pmmlUnivariateStats(pmmlElement):
  """"""
  def __init__(self, name="", attributes={}, children=[]):
    """"""
    types = [pmmlExtension, pmmlCounts, pmmlNumericInfo, pmmlDiscrStats, pmmlContStats]
    maximums = [None, 1, 1, 1, 1]
    minimums = [None, None, None, None, None]
    (extras, children) = pmmlSequence.formatChildren(types, maximums, children)
    if extras != []:
      raise pmmlError, "UnivariateStats:  " + pmmlErrorStrings.elements
    myChild = pmmlSequence(types, minimums, maximums, children)
    attributeNames = ["field"]
    pmmlElement.__init__(self, "UnivariateStats", myChild, attributeNames, attributes)

class pmmlModelStats(pmmlElement):
  """"""
  def __init__(self, name="", attributes={}, children=[]):
    """"""
    types = [pmmlExtension, pmmlUnivariateStats]
    maximums = [None, None]
    minimums = [None, 1]
    if attributes != {}:
      raise pmmlError, "ModelStats:  " + pmmlErrorStrings.noAttributes
    (extras, children) = pmmlSequence.formatChildren(types, maximums, children)
    if extras != []:
      raise pmmlError, "ModelStats:  " + pmmlErrorStrings.elements
    myChild = pmmlSequence(types, minimums, maximums, children)
    pmmlElement.__init__(self, "ModelStats", myChild)

class pmmlClassLabels(pmmlElement):
  """"""
  def __init__(self, name="", attributes={}, children=[]):
    """"""
    types = [pmmlExtension, pmmlArray]
    maximums = [None, 1]
    minimums = [None, 1]
    (extras, children) = pmmlSequence.formatChildren(types, maximums, children)
    if extras != []:
      raise pmmlError, "ClassLabels:  " + pmmlErrorStrings.elements
    myChild = pmmlSequence(types, minimums, maximums, children)
    attributeNames = []
    requiredAttributes = []
    pmmlElement.__init__(self, "ClassLabels", myChild, attributeNames, attributes, requiredAttributes)

class pmmlConfusionMatrix(pmmlElement):
  """"""
  def __init__(self, name="", attributes={}, children=[]):
    """"""
    types = [pmmlExtension, pmmlClassLabels, pmmlMatrix]
    maximums = [None, 1, 1]
    minimums = [None, 1, 1]
    (extras, children) = pmmlSequence.formatChildren(types, maximums, children)
    if extras != []:
      raise pmmlError, "ConfusionMatrix:  " + pmmlErrorStrings.elements
    myChild = pmmlSequence(types, minimums, maximums, children)
    attributeNames = []
    requiredAttributes = []
    pmmlElement.__init__(self, "ConfusionMatrix", myChild, attributeNames, attributes, requiredAttributes)

class pmmlXCoordinates(pmmlElement):
  """"""
  def __init__(self, name="", attributes={}, children=[]):
    """"""
    types = [pmmlExtension, pmmlArray]
    maximums = [None, 1]
    minimums = [None, 1]
    (extras, children) = pmmlSequence.formatChildren(types, maximums, children)
    if extras != []:
      raise pmmlError, "XCoordinates:  " + pmmlErrorStrings.elements
    myChild = pmmlSequence(types, minimums, maximums, children)
    attributeNames = []
    requiredAttributes = []
    pmmlElement.__init__(self, "XCoordinates", myChild, attributeNames, attributes, requiredAttributes)

class pmmlYCoordinates(pmmlElement):
  """"""
  def __init__(self, name="", attributes={}, children=[]):
    """"""
    types = [pmmlExtension, pmmlArray]
    maximums = [None, 1]
    minimums = [None, 1]
    (extras, children) = pmmlSequence.formatChildren(types, maximums, children)
    if extras != []:
      raise pmmlError, "YCoordinates:  " + pmmlErrorStrings.elements
    myChild = pmmlSequence(types, minimums, maximums, children)
    attributeNames = []
    requiredAttributes = []
    pmmlElement.__init__(self, "YCoordinates", myChild, attributeNames, attributes, requiredAttributes)

class pmmlBoundaryValues(pmmlElement):
  """"""
  def __init__(self, name="", attributes={}, children=[]):
    """"""
    types = [pmmlExtension, pmmlArray]
    maximums = [None, 1]
    minimums = [None, 1]
    (extras, children) = pmmlSequence.formatChildren(types, maximums, children)
    if extras != []:
      raise pmmlError, "BoundaryValues:  " + pmmlErrorStrings.elements
    myChild = pmmlSequence(types, minimums, maximums, children)
    attributeNames = []
    requiredAttributes = []
    pmmlElement.__init__(self, "BoundaryValues", myChild, attributeNames, attributes, requiredAttributes)

class pmmlBoundaryValueMeans(pmmlElement):
  """"""
  def __init__(self, name="", attributes={}, children=[]):
    """"""
    types = [pmmlExtension, pmmlArray]
    maximums = [None, 1]
    minimums = [None, 1]
    (extras, children) = pmmlSequence.formatChildren(types, maximums, children)
    if extras != []:
      raise pmmlError, "BoundaryValueMeans:  " + pmmlErrorStrings.elements
    myChild = pmmlSequence(types, minimums, maximums, children)
    attributeNames = []
    requiredAttributes = []
    pmmlElement.__init__(self, "BoundaryValueMeans", myChild, attributeNames, attributes, requiredAttributes)

class pmmlLiftGraph(pmmlElement):
  """"""
  def __init__(self, name="", attributes={}, children=[]):
    """"""
    types = [pmmlExtension, pmmlXCoordinates, pmmlYCoordinates, pmmlBoundaryValues, pmmlVoundaryValueMeans]
    maximums = [None, 1, 1, 1, 1]
    minimums = [None, 1, 1, None, None]
    (extras, children) = pmmlSequence.formatChildren(types, maximums, children)
    if extras != []:
      raise pmmlError, "LiftGraph:  " + pmmlErrorStrings.elements
    myChild = pmmlSequence(types, minimums, maximums, children)
    attributeNames = []
    requiredAttributes = []
    pmmlElement.__init__(self, "LiftGraph", myChild, attributeNames, attributes, requiredAttributes)

class pmmlModelLiftGraph(pmmlElement):
  """"""
  def __init__(self, name="", attributes={}, children=[]):
    """"""
    types = [pmmlExtension, pmmlLiftGraph]
    maximums = [None, 1]
    minimums = [None, 1]
    (extras, children) = pmmlSequence.formatChildren(types, maximums, children)
    if extras != []:
      raise pmmlError, "ModelLiftGraph:  " + pmmlErrorStrings.elements
    myChild = pmmlSequence(types, minimums, maximums, children)
    attributeNames = []
    requiredAttributes = []
    pmmlElement.__init__(self, "ModelLiftGraph", myChild, attributeNames, attributes, requiredAttributes)

class pmmlOptimumLiftGraph(pmmlElement):
  """"""
  def __init__(self, name="", attributes={}, children=[]):
    """"""
    types = [pmmlExtension, pmmlLiftGraph]
    maximums = [None, 1]
    minimums = [None, 1]
    (extras, children) = pmmlSequence.formatChildren(types, maximums, children)
    if extras != []:
      raise pmmlError, "OptimumLiftGraph:  " + pmmlErrorStrings.elements
    myChild = pmmlSequence(types, minimums, maximums, children)
    attributeNames = []
    requiredAttributes = []
    pmmlElement.__init__(self, "OptimumLiftGraph", myChild, attributeNames, attributes, requiredAttributes)

class pmmlRandomLiftGraph(pmmlElement):
  """"""
  def __init__(self, name="", attributes={}, children=[]):
    """"""
    types = [pmmlExtension, pmmlLiftGraph]
    maximums = [None, 1]
    minimums = [None, 1]
    (extras, children) = pmmlSequence.formatChildren(types, maximums, children)
    if extras != []:
      raise pmmlError, "RandomLiftGraph:  " + pmmlErrorStrings.elements
    myChild = pmmlSequence(types, minimums, maximums, children)
    attributeNames = []
    requiredAttributes = []
    pmmlElement.__init__(self, "RandomLiftGraph", myChild, attributeNames, attributes, requiredAttributes)

class pmmlLiftData(pmmlElement):
  """"""
  def __init__(self, name="", attributes={}, children=[]):
    """"""
    types = [pmmlExtension, pmmlModelLiftGraph, pmmlOptimumLiftGraph, pmmlRandomLiftGraph]
    maximums = [None, 1, 1, 1]
    minimums = [None, 1, None, None]
    (extras, children) = pmmlSequence.formatChildren(types, maximums, children)
    if extras != []:
      raise pmmlError, "LiftData:  " + pmmlErrorStrings.elements
    myChild = pmmlSequence(types, minimums, maximums, children)
    attributeNames = ["TargetFieldValue", "TargetFieldDisplayValue", "rankingQuality"]
    requiredAttributes = []
    pmmlElement.__init__(self, "LiftData", myChild, attributeNames, attributes, requiredAttributes)

class pmmlROCGraph(pmmlElement):
  """"""
  def __init__(self, name="", attributes={}, children=[]):
    """"""
    types = [pmmlExtension, pmmlXCoordinates, pmmlYCoordinates, pmmlBoundaryVaules]
    maximums = [None, 1, 1, 1]
    minimums = [None, 1, 1, None]
    (extras, children) = pmmlSequence.formatChildren(types, maximums, children)
    if extras != []:
      raise pmmlError, "ROCGraph:  " + pmmlErrorStrings.elements
    myChild = pmmlSequence(types, minimums, maximums, children)
    attributeNames = []
    requiredAttributes = []
    pmmlElement.__init__(self, "ROCGraph", myChild, attributeNames, attributes, requiredAttributes)

class pmmlROC(pmmlElement):
  """"""
  def __init__(self, name="", attributes={}, children=[]):
    """"""
    types = [pmmlExtension, pmmlROCGraph]
    maximums = [None, 1]
    minimums = [None, 1]
    (extras, children) = pmmlSequence.formatChildren(types, maximums, children)
    if extras != []:
      raise pmmlError, "ROC:  " + pmmlErrorStrings.elements
    myChild = pmmlSequence(types, minimums, maximums, children)
    attributeNames = ["positiveTargetFieldValue", "positiveTargetFieldDisplayValue", "negativeTargetFieldValue", "negativeTargetFieldDisplayValue"]
    requiredAttributes = ["positiveTargetFieldValue"]
    pmmlElement.__init__(self, "ROC", myChild, attributeNames, attributes, requiredAttributes)

class pmmlPredictiveModelQuality(pmmlElement):
  """"""
  def __init__(self, name="", attributes={}, children=[]):
    """"""
    types = [pmmlExtension, pmmlConfusionMatrix, pmmlLiftData, pmmlROC]
    maximums = [None, 1, 1, 1]
    minimums = [None, None, None, None]
    (extras, children) = pmmlSequence.formatChildren(types, maximums, children)
    if extras != []:
      raise pmmlError, "PredictiveModelQuality:  " + pmmlErrorStrings.elements
    myChild = pmmlSequence(types, minimums, maximums, children)
    attributeNames = ["targetField", "dataName", "dataUsage", "meanError", "meanAbsoluteError", "meanSquaredError", "r-squared"]
    requiredAttributes = ["targetField"]
    pmmlElement.__init__(self, "PredictiveModelQuality", myChild, attributeNames, attributes, requiredAttributes)

class pmmlClusteringModelQuality(pmmlElement):
  """"""
  def __init__(self, name="", attributes={}, children=[]):
    """"""
    types = []
    maximums = []
    minimums = []
    (extras, children) = pmmlSequence.formatChildren(types, maximums, children)
    if extras != []:
      raise pmmlError, "ClusteringModelQuality:  " + pmmlErrorStrings.elements
    myChild = pmmlSequence(types, minimums, maximums, children)
    attributeNames = ["dataname", "SSE", "SSB"]
    requiredAttributes = []
    pmmlElement.__init__(self, "ClusteringModelQuality", myChild, attributeNames, attributes, requiredAttributes)

class choicePC(pmmlChoice):
  """model quality group"""
  types = [pmmlPredictiveModelQuality, pmmlClusteringModelQuality]
  __maximums = [1, 1]
  def __init__(self, instances=[]):
    """"""
    minimums = [None, None]
    pmmlChoice.__init__(self, choicePC.types, minimums, choicePC.__maximums, instances)

  @staticmethod
  def formatChildren(children):
    """"""
    return pmmlChoice.formatChildren(choicePC.types, choicePC.__maximums, children)

class pmmlMatCell(pmmlElement):
  """"""
  def __init__(self, name="", attributes={}, children=[]):
    """"""
    types = []
    maximums = []
    minimums = []
    (extras, children) = pmmlSequence.formatChildren(types, maximums, children)
    if extras != []:
      raise pmmlError, "MatCell:  " + pmmlErrorStrings.elements
    myChild = pmmlSequence(types, minimums, maximums, children)
    attributeNames = ["row", "col"]
    requiredAttributes = ["row", "col"]
    pmmlElement.__init__(self, "MatCell", myChild, attributeNames, attributes, requiredAttributes)

class choiceAM(pmmlChoice):
  """Array or spare matrix group"""
  types = [pmmlArray, pmmlMatCell]
  __maximums = [None, None]
  def __init__(self, instances=[]):
    """"""
    minimums = [1, 1]
    pmmlChoice.__init__(self, choiceAM.types, minimums, choiceAM.__maximums, instances)

  @staticmethod
  def formatChildren(children):
    """"""
    return pmmlChoice.formatChildren(choiceAM.types, choiceAM.__maximums, children)

class pmmlMatrix(pmmlElement):
  """"""
  def __init__(self, name="", attributes={}, children=[]):
    """"""
    types = [choiceAM]
    maximums = [1]
    minimums = [None]
    (extras, children) = pmmlSequence.formatChildren(types, maximums, children)
    if extras != []:
      raise pmmlError, "Matrix:  " + pmmlErrorStrings.elements
    myChild = pmmlSequence(types, minimums, maximums, children)
    attributeNames = ["kind", "nbRows", "nbCols", "diagDefault", "offDiagDefault"]
    requiredAttributes = []
    pmmlElement.__init__(self, "Matrix", myChild, attributeNames, attributes, requiredAttributes)

class pmmlCorrelationFields(pmmlElement):
  """"""
  def __init__(self, name="", attributes={}, children=[]):
    """"""
    types = [pmmlExtension, pmmlArray]
    maximums = [None, 1]
    minimums = [None, 1]
    (extras, children) = pmmlSequence.formatChildren(types, maximums, children)
    if extras != []:
      raise pmmlError, "CorrelationFields:  " + pmmlErrorStrings.elements
    myChild = pmmlSequence(types, minimums, maximums, children)
    attributeNames = []
    requiredAttributes = []
    pmmlElement.__init__(self, "CorrelationFields", myChild, attributeNames, attributes, requiredAttributes)

class pmmlCorrelationValues(pmmlElement):
  """"""
  def __init__(self, name="", attributes={}, children=[]):
    """"""
    types = [pmmlExtension, pmmlMatrix]
    maximums = [None, 1]
    minimums = [None, 1]
    (extras, children) = pmmlSequence.formatChildren(types, maximums, children)
    if extras != []:
      raise pmmlError, "CorrelationValues:  " + pmmlErrorStrings.elements
    myChild = pmmlSequence(types, minimums, maximums, children)
    attributeNames = []
    requiredAttributes = []
    pmmlElement.__init__(self, "CorrelationValues", myChild, attributeNames, attributes, requiredAttributes)

class pmmlCorrelationMethods(pmmlElement):
  """"""
  def __init__(self, name="", attributes={}, children=[]):
    """"""
    types = [pmmlExtension, pmmlMatrix]
    maximums = [None, 1]
    minimums = [None, 1]
    (extras, children) = pmmlSequence.formatChildren(types, maximums, children)
    if extras != []:
      raise pmmlError, "CorrelationMethods:  " + pmmlErrorStrings.elements
    myChild = pmmlSequence(types, minimums, maximums, children)
    attributeNames = []
    requiredAttributes = []
    pmmlElement.__init__(self, "CorrelationMethods", myChild, attributeNames, attributes, requiredAttributes)

class pmmlCorrelations(pmmlElement):
  """"""
  def __init__(self, name="", attributes={}, children=[]):
    """"""
    types = [pmmlExtension, pmmlCorrelationFields, pmmlCorrelationValues, pmmlCorrelationMethods]
    maximums = [None, 1, 1, 1]
    minimums = [None, 1, 1, None]
    (extras, children) = pmmlSequence.formatChildren(types, maximums, children)
    if extras != []:
      raise pmmlError, "Correlations:  " + pmmlErrorStrings.elements
    myChild = pmmlSequence(types, minimums, maximums, children)
    attributeNames = []
    requiredAttributes = []
    pmmlElement.__init__(self, "Correlations", myChild, attributeNames, attributes, requiredAttributes)

class pmmlModelExplanation(pmmlElement):
  """"""
  def __init__(self, name="", attributes={}, children=[]):
    """"""
    types = [pmmlExtension, choicePC, pmmlCorrelations]
    maximums = [None, 1, 1]
    minimums = [None, 1, None]
    (extras, children) = pmmlSequence.formatChildren(types, maximums, children)
    if extras != []:
      raise pmmlError, "ModelExplanation:  " + pmmlErrorStrings.elements
    myChild = pmmlSequence(types, minimums, maximums, children)
    attributeNames = []
    requiredAttributes = []
    pmmlElement.__init__(self, "ModelExplanation", myChild, attributeNames, attributes, requiredAttributes)

class pmmlTargetValue(pmmlElement):
  """"""
  def __init__(self, name="", attributes={}, children=[]):
    """"""
    myChild = pmmlList([pmmlExtension], children)
    attributeNames = ["value","rawDataValue","priorProbability","defaultValue"]
    pmmlElement.__init__(self, "TargetValue", myChild, attributeNames, attributes)

class pmmlTarget(pmmlElement):
  """"""
  def __init__(self, name="", attributes={}, children=[]):
    """"""
    types = [pmmlExtension, pmmlTargetValue]
    maximums = [None, None]
    minimums = [None, None]
    (extras, children) = pmmlSequence.formatChildren(types, maximums, children)
    if extras != []:
      raise pmmlError, "Target:  " + pmmlErrorStrings.elements
    myChild = pmmlSequence(types, minimums, maximums, children)
    attributeNames = ["field","optype","castInteger","min","max","rescaleConstant","rescaleFactor"]
    requiredAttributes = ["field"]
    pmmlElement.__init__(self, "Target", myChild, attributeNames, attributes, requiredAttributes)

class pmmlTargets(pmmlElement):
  """"""
  def __init__(self, name="", attributes={}, children=[]):
    """"""
    types = [pmmlExtension, pmmlTarget]
    maximums = [None, None]
    minimums = [None, 1]
    if attributes != {}:
      raise pmmlError, "Targets:  " + pmmlErrorStrings.noAttributes
    (extras, children) = pmmlSequence.formatChildren(types, maximums, children)
    if extras != []:
      raise pmmlError, "Targets:  " + pmmlErrorStrings.elements
    myChild = pmmlSequence(types, minimums, maximums, children)
    pmmlElement.__init__(self, "Targets", myChild)

class pmmlVerificationField(pmmlElement):
  """"""
  def __init__(self, name="", attributes={}, children=[]):
    """"""
    myChild = pmmlList([pmmlExtension], children)
    attributeNames = ["field","column","precision","zeroThreshold"]
    requiredAttributes = ["field"]
    pmmlElement.__init__(self, "VerificationField", myChild, attributeNames, attributes, requiredAttributes)

class pmmlVerificationFields(pmmlElement):
  """"""
  def __init__(self, name="", attributes={}, children=[]):
    """"""
    types = [pmmlExtension, pmmlVerificationField]
    maximums = [None, None]
    minimums = [None, 1]
    if attributes != {}:
      raise pmmlError, "VerificationFields:  " + pmmlErrorStrings.noAttributes
    (extras, children) = pmmlSequence.formatChildren(types, maximums, children)
    if extras != []:
      raise pmmlError, "VerificationFields:  " + pmmlErrorStrings.elements
    myChild = pmmlSequence(types, minimums, maximums, children)
    pmmlElement.__init__(self, "VerificationFields", myChild)

class pmmlModelVerification(pmmlElement):
  """"""
  def __init__(self, name="", attributes={}, children=[]):
    """"""
    types = [pmmlExtension, pmmlVerificationFields, pmmlInlineTable]
    maximums = [None, 1, 1]
    minimums = [None, 1, 1]
    (extras, children) = pmmlSequence.formatChildren(types, maximums, children)
    if extras != []:
      raise pmmlError, "ModelVerification:  " + pmmlErrorStrings.elements
    myChild = pmmlSequence(types, minimums, maximums, children)
    attributeNames = ["recordCount","fieldCount"]
    pmmlElement.__init__(self, "ModelVerification", myChild, attributeNames, attributes)
