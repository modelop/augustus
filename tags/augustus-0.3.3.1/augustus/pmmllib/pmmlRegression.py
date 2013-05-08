from pmmlElements import *
from pmmlModelElements import *
from numpy import *
from augustus.kernel.unitable import *
"""
 pmmlRegression captures all of the functionality that varies on
 a per-segment basis. In particular, it carries the scoring function and
 any special functionality needed when updating with new data.
 On initialization, it also returns specification of associated
 explicit segments. These can be used (by producers and consumers) to
 sort through a set of pmmlRegressions.

 Public methods:
  initialize(dataInput, directFields, predictedField, localXforms)
  getRestrictions() (segment rules)
  getSortedRestrictions() (alpha-sorted on segment fieldnames)
  makeFunction(predictedField)
  score(datamatrix)

 It is anticipated that future specs for PMML will use it as an "embedded" 
 element with either a MiningModel or MiningModel+child segments structure. 
 Compare to relationship between TreeModel and DecisionTree.


"""
class pmmlRegression(pmmlElement):
  """"""
  def __init__(self, name="", attributes={}, children=[]):
    """"""
    types = [pmmlExtension,pmmlRegressionTable,pmmlSegments]
    maximums = [None, None, None]
    minimums = [None, 1, 0]
    (extras, children) = pmmlSequence.formatChildren(types, maximums, children)
    if extras != []:
      raise pmmlError, "Regression:  " + pmmlErrorStrings.elements
    myChild = pmmlSequence(types, minimums, maximums, children)
    attributeNames = []
    requiredAttributes = []
    pmmlElement.__init__(self, "Regression", myChild, attributeNames, attributes, requiredAttributes)

  def initialize(self, dataInput, directFields, predictedField, localTransDict=None):
    """ Carries out three tasks: 
        1. Create scoring functionality
        2. Identify 'Update' functionality
        3. Parse segment information, returning explicit segment data
           to help caller iterate/sort through full set of regressions.
    """
    self.__set = self.makeFunction(predictedField)
    self.__directFields = directFields    
    #if the function will never return a score
    if not self.__set:
      return None
    self.__getMining = dataInput.__getitem__
    #set up for no localTransformations
    if localTransDict is None:
      self.get = self.__getMining
      self.update = self.__update
      self.__localTransDictUpdate=self.__doNothing
      self.match = self.__matchLocal
      self.revert = self.__doNothing
      self.lastUpdate = self.__doNothing
    #set up for localTransformations
    else:
      self.__getLocalTrans = localTransDict.__getitem__
      self.get = self.__getLocal
      self.__localTransDictUpdate=localTransDict.update
      self.update = self.__update
      self.match = self.__matchLocal
      self.revert = localTransDict.revert
      self.lastUpdate = localTransDict.lastUpdate
      self.limit = localTransDict.limit
      self.force = localTransDict.force
    #get keys for faster sorting
    rules = self.getRestrictions()
    ruleFields = rules.keys()
    # Identify Explicit segment fields and values
    # available as *raw* data.
    # These are returned to caller to help them
    # identify this regression instance.
    explicitSegFields = []
    explicitSegValues = []
    for field in directFields:
      if field in ruleFields:
        rulevalue = rules[field]
        if isinstance(rulevalue, str):
          explicitSegFields.append(field)
          explicitSegValues.append(rulevalue)
    #Record the rules that will still need to be checked
    #These are primarily regular segments.
    self.__orig = rules
    self.__rules = {}
    for field in rules:
      if not field in explicitSegFields:
        self.__rules[field] = rules[field]
    #check to see if we should skip creating a score n times
    extensions = self.getChildrenOfType(pmmlExtension)
    self.__number = None
    if extensions:
      for extension in extensions:
        skip = extension.getChildrenOfType(extensionSkip)
        if skip:
          skip = skip[0]
          self.__number = long(skip.getAttribute("number"))
          self.__cnt = 0
          break
    #return the keys needed for quicker sorting
    self.vars, self.powers = self.getPredictors()
    return (tuple(explicitSegFields), tuple(explicitSegValues))

  def getPredictors(self):
    return self.__set[1],self.__ancillary['exponents']
  
  def __threshold(value, standard):
    """"""
    return (value, value >= standard)
  __threshold = staticmethod(__threshold)

  def __belowthreshold(value, standard):
    """"""
    return (value, value <= standard)
  __belowthreshold = staticmethod(__belowthreshold)
  
  def getSortedRestrictions(self):
    segrules = self.getRestrictions()
    segfields = segrules.keys()
    segfields.sort()
    return tuple([(key,segrules[key]) for key in segfields])
    

  def getRestrictions(self):
    """"""
    restrictions = {}
    segments = self.getChildrenOfType(pmmlSegments)
    if len(segments) == 1:
      segments = segments[0]
      segments = segments.getChildren()
      for segment in segments:
        if isinstance(segment, pmmlExplicitSegment):
          restrictions[segment.getAttribute("field")] = segment.getAttribute("value")
        else:
          restrictions[segment.getAttribute("field")] = (float(segment.getAttribute("low")), float(segment.getAttribute("high")))
    return restrictions

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
      intercept
      numeric predictors
      categorical predictors
      interactions
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

    regtable = self.getChildrenOfType(pmmlRegressionTable)[0]
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

    function = lambda current : pmmlRegression.__predict(current, predictors, interactions)
    if function:
       self.__ancillary['exponents'] = powers
       set = [0, otherfields, function, self.__ancillary]
    return set

  def __getLocal(self, field):
    """"""
    if field in self.__directFields:
      return self.__getMining(field)
    return self.__getLocalTrans(field)
  
  def __matchLocal(self):
    """"""
    met = True
    for field in self.__rules:
      rule = self.__rules[field]
      value = self.get(field)
      if value is NULL:
        met = False
        break
      if isinstance(rule, str):
        if str(value) != rule:
          met = False
          break
      elif value < rule[0] or value > rule[1]:
        met = False
        break
    return met
  
  #def score(self, rowtoscore, predictedfield):
  def score(self, toscore):
    """
    The ancillary dictionary is referenced but not
    currently used. This is the
    general mechanism for accessing any output data
    which is not explicitly part of a score.
    """
    interactions = {}
    requestedInteractions = self.getChildrenOfType(pmmlRegressionTable)[0].getInteractions()
    polynomialterms = {}
    for interaction in requestedInteractions:            
      if len(interaction)!=2:
        print 'Sorry-only bivariate interactions are currently supported\n'
        raise
      #interactionName = tuple(interaction)
      #try:
      #  toscore[interactionName] = interactions[tuple(interactionName)]
      #except:
      #  print interactions
      #  x=input()
      prod=1.0
      for f in interaction:
        prod*=toscore[f]
      interactions[tuple(interaction)] = prod
      #field0 = interaction[0]
      #field1 = interaction[1]
      #interactions[(field0,field1)]=toscore[field0]*toscore[field1]
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
        if self.__number:
          self.__number -= 1
        else:
          answer.append((temp, True, self.__orig, self.__ancillary))
    return answer
  
  def __update(self):
    """"""
    self.__localTransDictUpdate()
    nvalue = [self.get(var) for var in self.__set[1]]
    return self.__orig,nvalue

  def __doNothing(self):
    """"""
    pass
  
  def __returnTrue(self):
    """"""
    return True


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

