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
from pmmlModelElements import *
from numpy import *
import os
from augustus.kernel.unitable import *
"""
TODO: Re-write this documentation.
 pmmlRegression captures all of the functionality that varies on
 a per-segment basis. In particular, it carries the scoring function and
 any special functionality needed when updating with new data.
 On initialization, it also returns specification of associated
 explicit segments. These can be used (by producers and consumers) to
 sort through a set of pmmlRegressions.

 Public methods:
  initialize(dataInput, directFields, predictedField, localXforms)
  makeFunction(predictedField)
  score(datamatrix)

 It is anticipated that future specs for PMML will use it as an "embedded" 
 element with either a MiningModel or MiningModel+child segments structure. 
 Compare to relationship between TreeModel and DecisionTree.


"""
class Regression:
  """Helper class that wraps up the pmmlRegressionTable elements into the logical unit that they are."""
  def __init__(self, tables, predictedField):
    """"""
    self.tables = tables
    self.predictedField = predictedField

  def initialize(self, get, dataInput, localTransDict=None, segment = None):
    """ Carries out three tasks:

        1. Create scoring functionality
        2. Identify 'Update' functionality
        3. Parse segment information, returning explicit segment data
           to help caller iterate/sort through full set of regressions.

    """
    self.get = get
    self.__set = self.makeFunction(self.predictedField)
    #if the function will never return a score
    if not self.__set:
      return None
    self.vars, self.powers = self.getPredictors()
    
    if segment:
      self.segment = segment
    else:
      self.segment = {}

  def getPredictors(self):
    """"""
    return self.__set[1],self.__ancillary['exponents']

  @staticmethod  
  def __predict(current, tMatrix, interactions = []):
    """"""
    return current.T*tMatrix

  def makeFunction(self,predictedField):
    """
    Create Scoring function for this segment.
    Requires the predicted field as an argument.
    The scoring function is implemented as a vector
    of coefficients, in order:

      1. intercept
      2. numeric predictors
      3. categorical predictors
      4. interactions

    Within each category, the order is in the order
    of the pmml-specified elements. Probably should
    be changed to ensure interoperability.
    """
    set = None
    self.__ancillary = {}
    otherfields = []
    interactions = []
    missingValueStrategy = None
    missingValue = object()

    regtable = self.tables[0]
    #predictors = [float(regtable.getAttribute('intercept'))]
    powers = []

    for numpred in regtable.getChildrenOfType(pmmlNumericPredictor):
      try:
        predictors.append(float(numpred.getAttribute('coefficient')))
      except:
        predictors = [float(numpred.getAttribute('coefficient'))]
      predictorname = numpred.getAttribute('name')
      if predictorname not in otherfields:
        otherfields.append(predictorname)
      try:
        powers.append((predictorname,numpred.getAttribute('exponent')))
      except:
        # exponent is *not* a required attribute in pmml and defaults
        # to 1.
        powers.append("1")

    for catpred in regtable.getChildrenOfType(pmmlCategoricalPredictor):
      predictors.append(float(catpred.getAttribute('coefficient')))
      otherfields.append(catpred.getAttribute('name'))

    # untested
    for interaction in regtable.getChildrenOfType(pmmlPredictorTerm):
      interactionCoefficient = interaction.getAttribute('coefficient')
      interactingFields = []
      for field in interaction.getChildrenOfType(pmmlFieldRef):
        interactingFields.append(field.getAttribute('field'))
      interactions.append(interactingFields)
      predictors.append(float(interactionCoefficient))

    otherfields.append(predictedField)
    predictors.append(float(regtable.getAttribute('intercept')))
    predictors = numpy.matrix(predictors).reshape(len(predictors),1)

    function = lambda current : Regression.__predict(current, predictors, interactions)
    if function:
       self.__ancillary['exponents'] = powers
       set = [0, otherfields, function, self.__ancillary]
    return set

  def score(self):
    """
    The ancillary dictionary is referenced but not
    currently used. This is the
    general mechanism for accessing any output data
    which is not explicitly part of a score.
    """
    toscore = EvalTable(keys = self.vars)
    toscore.append([self.get(var) for var in self.__set[1]])
    dependent = toscore.pop(self.predictedField)
    
    interactions = {}
    requestedInteractions = self.tables[0].getInteractions()
    polynomialterms = {}
    for interaction in requestedInteractions:
      if len(interaction)!=2:
        print 'Sorry-only bivariate interactions are currently supported' + os.linesep
        raise
      prod=1.0
      for f in interaction:
        prod*=toscore[f]
      interactions[tuple(interaction)] = prod
    # Re-calculate inputs accounting for multiple powers.
    for p in self.powers:
      polynomialterms[p[0]+'_'+p[1]] = toscore[p[0]]**float(p[1])
      toscore.add_rule(p[0]+'_'+p[1]+'='+p[0]+'**'+p[1])
    # force evaluation of raw data in evaltable
    [toscore[k] for k in toscore.rule_keys()]
    # exclude raw values.
    for k in toscore.keys():
      if (k not in polynomialterms):
        toscore.pop(k)
    # Add the interaction calculations
    for interaction in requestedInteractions:
      interactionName = '*'.join(interaction)
      toscore[interactionName] = interactions[tuple(interactionName.split('*'))]
      #field0 = interaction[0]
      #field1 = interaction[1]
      #toscore[field0+'*'+field1]=interactions[(field0,field1)]
    # constant term
    toscore.newfield('intercept',1.0)
    values = toscore.values()
    answer = []
    if not isinstance(values, matrix):
      values = matrix(values)
    if not values is NULL and not values is None:
      temp = self.__set[2](values)  # call scoring function
      if not temp is None:
        self.__set[0] = temp
        answer.append((temp[0,0], True, self.segment, self.__ancillary))
    return answer

class pmmlRegressionTable(pmmlElement):
  def __init__(self, name="", attributes={}, children=[]):
    """define and instantiate the element"""
    types = [pmmlExtension, pmmlNumericPredictor, pmmlCategoricalPredictor,pmmlPredictorTerm]
    maximums = [None, None, None, None]
    minimums = [None, None, None, None]
    (extras, children) = pmmlSequence.formatChildren(types, maximums, children)
    if extras != []:
      raise pmmlError, "pmmlRegressionTable: " + pmmlErrorStrings.elements
    myChild = pmmlSequence(types, minimums, maximums, children)
    attributeNames = ["intercept", "targetCategory"]
    requiredAttributes = ["intercept"]
    pmmlElement.__init__(self, "RegressionTable", myChild, attributeNames, attributes, requiredAttributes)
  
  def getInteractions(self):
    """
     Return mapping of fieldnames for each interactin to the respective coefficient.
    """
    interactions = {}
    for interaction in self.getChildrenOfType(pmmlPredictorTerm):
      fields = [f.getAttribute('field') for f in interaction.getChildrenOfType(pmmlFieldRef)]
      fields.sort()
      fields = tuple(fields)
      interactions[fields] = interaction.getAttribute('coefficient')
    return interactions

class pmmlNumericPredictor(pmmlElement):
  def __init__(self, name="", attributes={}, children=[]):
    """define and instantiate the element"""
    myChild = pmmlList([pmmlExtension], children)
    attributeNames = ["name", "exponent", "coefficient"]
    requiredAttributes = ["name", "coefficient"]
    pmmlElement.__init__(self, "NumericPredictor", myChild, attributeNames, attributes, requiredAttributes)

class pmmlCategoricalPredictor(pmmlElement):
  def __init__(self,name="", attributes={}, children=[]):
    """define and instantiate the element"""
    myChild = pmmlList([pmmlExtension], children)
    attributeNames = ["name", "value", "coefficient"]
    requiredAttributes = ["name", "value", "coefficient"]
    pmmlElement.__init__(self, "CategoricalPredictor", myChild, attributeNames, attributes, requiredAttributes)

class pmmlPredictorTerm(pmmlElement):
  def __init__(self,name="", attributes={}, children=[]):
    """define and instantiate the element"""
    types = [pmmlExtension, pmmlFieldRef]
    maximums = [None, None]
    minimums = [None, 1]
    (extras, children) = pmmlSequence.formatChildren(types, maximums, children)
    if extras != []:
      raise pmmlError, "pmmlPredictorTerm: " + pmmlErrorStrings.elements
    myChild = pmmlSequence(types, minimums, maximums, children)
    attributeNames = ["coefficient"]
    requiredAttributes = ["coefficient"]
    pmmlElement.__init__(self, "PredictorTerm", myChild, attributeNames, attributes, requiredAttributes)

