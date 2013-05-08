#!/usr/bin/env python2.5

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

from CommonProducer import *

class Producer(CommonProducer):
  """"""
  def __init__(self, timing = None, wrapper = True):
    CommonProducer.__init__(self, timing, wrapper)

  
  def inputConfigs(self, file):
    """
    As for all producers, configuration file specifies:
    -Data to be used to build model
    -Segmentation
    -Skeleton PMML
    -Output PMML file
    """
    CommonProducer.inputConfigs(self, file)

  def inputPMML(self):
    """
    Parse elements of 'skeleton' pmml file specifying
    how model should be constructed.
    """
    CommonProducer.inputPMML(self, pmmlRegressionModel)

    # the _existingTests child is a hook into an anticipated 
    # need to build a model by updating a pre-existing one.
    self._existingTests = self._model.getChildrenOfType(pmmlRegression)
    self._regtablePrototype=self._existingTests[0].getChildrenOfType(pmmlRegressionTable)[0]
    self._interactions={}
    interactions = self._regtablePrototype.getChildrenOfType(pmmlPredictorTerm)
    for interaction in interactions:
      fields = []
      for f in interaction.getChildrenOfType(pmmlFieldRef):
        fields.append(f.getAttribute('field'))
      fields.sort()
      fields=tuple(fields)
      self._interactions[fields]=interaction.getAttribute('coefficient')

  
  @staticmethod
  def _addResults(teststat, results, stats, regressionData):
    """
     Use Cholesky decomposition to estimate linear regression parameters.
    """
    # Notice that this function modifies the stats dictionary
    # as though it were passed by reference.
    for result in results:
      rules = Producer.tupelize(result[2])
      dependent = matrix(result[3]['dependent'])
      dependent = dependent.reshape(len(result[3].values()[0]),1)
      independent = matrix(regressionData[rules].values())
      bvector = independent*dependent
      m2 = independent*independent.T
      L = numpy.linalg.cholesky(m2)
      zvector = numpy.linalg.solve(L, bvector)
      beta = numpy.linalg.solve(L.T, zvector)
      residuals = independent.T*beta-dependent
      #
      #print 'Residuals: '
      #print residuals
      stats[rules] = [beta, dependent, residuals]
  
  def _score(self, tables):
    """
    Construct 'dummy' model for all segments, batch data and score it.
    Scoring produces data used for subsequent regression.
    """

    #set up the PMML
    teststat = None
    regtable = self._regtablePrototype
    self.specifySegments(regtable, pmmlRegression)

    self._pmml.initialize(self.get, [], self._batch)

    vars, pows = self._model.getChildrenOfType(pmmlRegression)[0].getPredictors()
    self.regressionvariables = vars


    # statistics and measurement matrices indexed by segment
    stats = dict(self._baseDict) 
    regressionData = dict(self._baseDict)

    #Scoring of events:
    for table in tables:
      if self._timer and len(table) > 0:
        total = len(table)
        increment = self.__increment * total
        increment = int(math.ceil(increment))
        threshold = increment
        #acts as the shift for the % calculation later
        perc = float(100) / total
        cnt = 0
        last = 0
        for row in table:
          self.row = row
          rowstoscore = self._model.batchEvent()
          for rowtoscore in rowstoscore:
            regression = CommonProducer.tupelize(rowtoscore[0])
            if regressionData[regression] is not None:
              regressionData[regression].append(rowtoscore[1])
            else:
              regressionData[regression] = EvalTable(keys=self.regressionvariables)
              regressionData[regression].append(rowtoscore[1])
          cnt += 1
          if cnt == threshold:
            num = "%.3f" % (cnt * perc)
            last = float(num)
            self._timer.output("Events " + num + "% processed")
            threshold += increment
        # feed the model all of the data
        results = self._model.batchScore(regressionData,self._interactions) 
        Producer._addResults(teststat, results, stats, regressionData)
        if last < 100.000:
          self._timer.output("Events 100.000% processed")
      else:
        for row in table:
          self.row = row
          rowstoscore = self._model.batchEvent()
          for rowtoscore in rowstoscore:
            regression = Producer.tupelize(rowtoscore[0])
            if regressionData[regression] is not None:
              regressionData[regression].append(rowtoscore[1])
            else:
              regressionData[regression] = EvalTable(keys=self.regressionvariables)
              regressionData[regression].append(rowtoscore[1])
        # feed the model all of the data
        results = self._model.batchScore(regressionData, self._interactions) 
        #print stats
        Producer._addResults(teststat, results, stats, regressionData)
    #print self._stats
    #print stats
    return stats
      
  def getStats(self):
    """ Collect statistics produced in process of scoring. """
    if self._timer:
      self._timer.output("Collecting statistics for model")
    self._stats = self._score(self._makeTable(self._build))
    self.__cur = -time.time()
  
  @staticmethod
  def makeRegressionPredictors(fit, regressionVariables=[], interactionsRequested={}, powers=[], testvalidation = None):
    """
    Method to construct list of pmml elements describing polynomial and
    interacton terms for regression using pmmlNumericPredictors and
    pmmlPredictorTerms. Requires fit results, variables, interactions, and
    exponents.    """
    # stats[0]=regression array(intercept, beta1,...)
    # stats[1]=regression input matrix
    # stats[2]=measured dependent values
    #self._model.getChildrenOfType(pmmlRegression)[0]
    inAttributes = []
    outAttributes = []    
    predictors = []
    iterm = 0    
    for term in powers:
       #exponent = powers[iterm][1]
       exponent = term[1]
       name = term[0]
       coefficient = fit[iterm,0]
       iterm+=1
       predictors.append(pmmlNumericPredictor(attributes={'name':name,'exponent':str(exponent),'coefficient':str(coefficient)}))
    interactions=[]
    for interactionfields in interactionsRequested.keys():
      children=[]
      for field in interactionfields:
        children.append(pmmlFieldRef(attributes={'field':field}))
      interactions.append(pmmlPredictorTerm(attributes={'coefficient':str(fit[iterm,0])},children=children))
      iterm+=1
    return predictors+interactions
  
  def makeSegments(aTuple):
    """"""
    segments = []
    for pair in aTuple:
      field = pair[0]
      value = pair[1]
      if isinstance(value, str):
        segments.append(pmmlExplicitSegment(attributes={"field":field,"value":value}))
      else:
        segments.append(pmmlRegularSegment(attributes={"field":field,"low":str(value[0]),"high":str(value[1])}))
    return segments
  makeSegments = staticmethod(makeSegments)
  
  def makeRegressions(self):
    """"""
    if self._timer:
      self._timer.output("Making tests from statistics")
    #extensions
    extensions = []
    if self._skip:
      extensions.append(pmmlExtension(children=[extensionSkip(attributes={"number":str(self._skip)})]))
    #create a test for each segment
    tests = []
    originals = self._model.getChildrenOfType(pmmlRegression)
    powers = self._model.powers
    for o in originals:
      entry = o.getSortedRestrictions()
      thisfit = self._stats[o.getSortedRestrictions()][0]
      intercept = thisfit[-1,-1]
      children = Producer.makeRegressionPredictors(thisfit, self.regressionvariables, self._interactions, powers, self.testValidation)
      if children: 
        regtable = pmmlRegressionTable(attributes={'intercept':str(intercept)},children=children)
        segments = Producer.makeSegments(entry)
        segments = pmmlSegments(children=segments)
        children = list(extensions)
        children.extend([regtable,segments])
        tests.append(pmmlRegression(children=children, attributes=self._attrs))
    self._model.addChildren(tests)
    for o in originals:
      self._model.removeChild(o)
  
  def outputPMML(self):
    """"""
    if self._timer:
      self._timer.output("Outputting PMML")
    #output the model with the segments that have been produced
    out = open(self._output,"w")
    out.write(str(self._pmml))
    out.close()
  
  def stop(self):
    """"""
    if self._timer:
      del self._timer

def main(config=None, timing=None, wrapping=True):
  """"""
  #define the options
  usage = "usage: %prog [options]"
  version = "%prog 0.3.0"
  options = [
    make_option("-c","--config",default="config.xml",help="The configuration file name"),
    make_option("-t","--timing",default=None,help="Output timing (information in % increments for scoring)")]
  parser = OptionParser(usage=usage, version=version, option_list=options)
  
  #parse the options
  if not config:
    (options, arguments) = parser.parse_args()
    config = options.config
    timing = options.timing
  
  #make a producer
  mine = Producer(timing, wrapping)
  #input the configurations
  mine.inputConfigs(config)
  #input the PMML
  mine.inputPMML()
  #create the statistics
  mine.getStats()
  #make the Regression elements
  mine.makeRegressions()
  #output the PMML
  mine.outputPMML()
  #stop timing if it is going
  mine.stop()

if __name__ == "__main__":
  main()
