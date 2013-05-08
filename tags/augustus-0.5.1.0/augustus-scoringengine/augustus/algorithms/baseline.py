#!/usr/bin/env python

# Copyright (C) 2006-2011  Open Data ("Open Data" refers to
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

"""Defines the baseline producer and consumer algorithms."""

# system includes
import math

# local includes
from augustus.algorithms.defs import ConsumerAlgorithm, ProducerAlgorithm
from augustus.algorithms.eventweighting import UpdateScheme, COUNT, SUM1, SUMX, SUMXX, CUSUM, GLR
from augustus.core.defs import INVALID, MISSING, Atom
from augustus.core.extramath import erfinv, chiSquare_cdf

import augustus.core.pmml41 as pmml

# every testStatistic produces a predictedValue
SCORE_predictedValue = pmml.OutputField.predictedValue

# some provide additional information in ODG extensions
SCORE_pValue = pmml.X_ODG_OutputField.pValue
SCORE_chiSquare = pmml.X_ODG_OutputField.chiSquare
SCORE_degreesOfFreedom = pmml.X_ODG_OutputField.degreesOfFreedom
SCORE_thresholdTime = pmml.X_ODG_OutputField.thresholdTime

#########################################################################################
######################################################################### consumer ######
#########################################################################################

class ConsumerBaselineModel(ConsumerAlgorithm):
    CHISQUAREDISTRIBUTION = Atom("chiSquareDistribution")
    SCALARPRODUCT = Atom("scalarProduct")
    INDEPENDENT = Atom("Independent")
    SIZEWEIGHTED = Atom("SizeWeighted")

    def initialize(self):
        """Initialize a baseline consumer.

        Unlike other consumers, this creates the score function
        dynamically, depending on the type of testStatistic.
        """

        testDistributions = self.segmentRecord.pmmlModel.child(pmml.TestDistributions)
        self.field = testDistributions.attrib["field"]
        testStatistic = testDistributions.attrib["testStatistic"]

        # updating can be configured in the Augustus configuration file and in the "windowSize" attribute in this segment
        # I will assume that the "windowSize" attribute can override CUSUM and GLR only

        # (the only other baseline consumer that has an intermediate state is chiSquareDistribution, which only makes
        # sense as UpdateScheme("synchronized"), and that one depends on the configuration of syncNumber, not in PMML)

        # the general case:
        self.updateScheme = self.engine.consumerUpdateScheme
        # the special case:
        if testStatistic in ("CUSUM", "GLR"):
            if "windowSize" in testDistributions.attrib and testDistributions.attrib["windowSize"] != 0:
                self.updateScheme = UpdateScheme("window", windowSize=testDistributions.attrib["windowSize"], windowLag=0)

        if testStatistic == "CUSUM":
            self.baseline = testDistributions.child(pmml.Baseline).child()
            self.alternate = testDistributions.child(pmml.Alternate).child()
            self.updator = self.updateScheme.updator(CUSUM)
            self.updator.resetValue = testDistributions.attrib["resetValue"]
            self.score = self.scoreCUSUM

            extension = testDistributions.child(pmml.Extension, exception=False)
            if extension is not None:
                init = extension.child(pmml.X_ODG_CUSUMInitialization, exception=False)
                if init is not None:
                    self.updator.initialize({CUSUM: [init.attrib["value"]]})
                
            self.pseudoField = self.field
            self.pseudoOutputAll = True

        elif testStatistic == "zValue":
            self.baseline = testDistributions.child(pmml.Baseline).child()
            if isinstance(self.baseline, pmml.GaussianDistribution):
                self.score = self.scoreZValueGaussian
            else:
                self.score = self.scoreZValue

            self.pseudoField = self.field
            self.pseudoOutputAll = True

        elif testStatistic in ("chiSquareDistribution", "scalarProduct"):
            self.updators = {}
            self.countTable = testDistributions.child(pmml.Baseline).child()

            if "weightField" in testDistributions.attrib:
                self.weightField = testDistributions.attrib["weightField"]
            else:
                self.weightField = None

            if "normalizationScheme" not in testDistributions.attrib:
                self.normalizationScheme = None
            elif testDistributions.attrib["normalizationScheme"] == "Independent":
                self.normalizationScheme = self.INDEPENDENT
            elif testDistributions.attrib["normalizationScheme"] == "SizeWeighted":
                self.normalizationScheme = self.SIZEWEIGHTED

            self.testStatistic = {"chiSquareDistribution": self.CHISQUAREDISTRIBUTION,
                                  "scalarProduct": self.SCALARPRODUCT,
                                  }[testStatistic]

            self.score = self.scoreHistogram

            self.pseudoField = (self.field, self.weightField)
            self.pseudoOutputAll = False

            # ODG extensions
            self.binsOfInterest = testDistributions.descendant(pmml.X_ODG_BinsOfInterest, exception=False)

        elif testStatistic == "chiSquareIndependence":
            self.baseline = testDistributions.child(pmml.Baseline)
            self.fields = None
            self.countTable = None
            self.score = self.scoreChiSquareIndependence

            self.pseudoField = None
            self.pseudoOutputAll = True

        # ODG extensions
        elif testStatistic == "GLR":
            self.baseline = testDistributions.child(pmml.Baseline).child()
            if not isinstance(self.baseline, (pmml.GaussianDistribution, pmml.PoissonDistribution)):
                raise NotImplementedError, "GLR has only been implemented for Gaussian and Poisson distributions"

            self.updator = self.updateScheme.updator(GLR)
            self.score = self.scoreGLR

            self.pseudoField = self.field
            self.pseudoOutputAll = False

    ######################################## CUSUM

    def scoreCUSUM(self, syncNumber, get):
        """Score one event with a CUSUM testStatistic."""

        value = get(self.field)
        if value is INVALID or value is MISSING:
            return INVALID

        self.updator.increment(syncNumber, self.alternate.logpdf(value) - self.baseline.logpdf(value))
        return {SCORE_predictedValue: self.updator.cusum()}

    ######################################## zValue

    def scoreZValueGaussian(self, syncNumber, get):
        """Score one event with a zValue testStatistic (Gaussian)."""

        value = get(self.field)
        if value is INVALID or value is MISSING:
            return INVALID

        if self.baseline.attrib["variance"] == 0.:
            return {SCORE_predictedValue: float("inf"), SCORE_pValue: 0.}

        elif self.baseline.attrib["variance"] < 0.:
            return INVALID

        zValue = (value - self.baseline.attrib["mean"]) / math.sqrt(self.baseline.attrib["variance"])
        probability = self.baseline.cdf(value)
        pValue = 1. - 2.*abs(probability - 0.5)

        return {SCORE_predictedValue: zValue, SCORE_pValue: pValue}

    def scoreZValue(self, syncNumber, get):
        """Score one event with a zValue testStatistic (non-Gaussian)."""

        value = get(self.field)
        if value is INVALID or value is MISSING:
            return INVALID

        probability = self.baseline.cdf(value)
        if probability <= 1e-16:
            zValue = -10.
        elif probability >= 1. - 1e-16:
            zValue = 10.
        else:
            zValue = math.sqrt(2.)*erfinv(2.*probability - 1.)
        pValue = 1. - 2.*abs(probability - 0.5)

        return {SCORE_predictedValue: zValue, SCORE_pValue: pValue}

    ######################################## chiSquareDistribution and scalarProduct

    def scoreHistogram(self, syncNumber, get):
        """Score one event with a chiSquareDistribution or scalarProduct."""

        value = get(self.field)
        if self.weightField is None:
            weight = 1.
        else:
            weight = get(self.weightField)

        # we can still calculate the consistency of the *accumulated* distribution, even if this datapoint is invalid
        if value is INVALID or value is MISSING or weight is INVALID or weight is MISSING:
            pass
        else:
            # for histograms, increment all bins, but only the correct bin gets a non-zero value
            found = False
            for bin, updator in self.updators.items():
                if bin == value:
                    updator.increment(syncNumber, weight)
                    found = True
                else:
                    updator.increment(syncNumber, 0.)

            # this might be a new bin
            if not found:
                updator = self.updateScheme.updator(SUMX)
                updator.increment(syncNumber, weight)
                self.updators[value] = updator
            
        fieldValueCounts = self.countTable.matches(pmml.FieldValueCount, maxdepth=None)

        # chiSquareDistribution
        if self.testStatistic == self.CHISQUAREDISTRIBUTION:
            expectedTotal = 0.
            expectedValues = {}
            for fieldValueCount in fieldValueCounts:
                bin = fieldValueCount.attrib["value"]
                count = fieldValueCount.attrib["count"]
                expectedTotal += count
                expectedValues[bin] = count

            observedTotal = 0.
            for bin, updator in self.updators.items():
                observedTotal += updator.sum()

            if expectedTotal <= 0. or observedTotal <= 0. or (isinstance(self.countTable, pmml.NormalizedCountTable) and self.countTable.attrib["sample"] <= 0.):
                return INVALID

            chi2 = 0.
            if self.binsOfInterest is None:
                ndf = -1  # normalization removes one degree of freedom
            else:
                ndf = 0

            for bin in set(expectedValues.keys()).union(set(self.updators.keys())):
                if self.binsOfInterest is not None:
                    if bin not in self.binsOfInterest:
                        continue
                
                expected = expectedValues.get(bin, 0.)
                updator = self.updators.get(bin, None)
                if updator is not None:
                    observed = updator.sum()
                else:
                    observed = 0.

                if expected > 0. or observed > 0.:
                    if isinstance(self.countTable, pmml.CountTable):
                        chi2 += (expected/expectedTotal - observed/observedTotal)**2 / (expected/expectedTotal**2 + observed/observedTotal**2)

                    elif isinstance(self.countTable, pmml.NormalizedCountTable):
                        sample = self.countTable.attrib["sample"]
                        chi2 += (expected/expectedTotal - observed/observedTotal)**2 / (expected/expectedTotal/sample + observed/observedTotal**2)

                    ndf += 1

            if ndf > 0:
                probability = chiSquare_cdf(chi2, ndf)
                pValue = 1. - probability
                return {SCORE_predictedValue: probability, SCORE_pValue: pValue, SCORE_chiSquare: chi2, SCORE_degreesOfFreedom: ndf}
            else:
                return INVALID

        # scalarProduct
        elif self.testStatistic == self.SCALARPRODUCT:
            expectedNorm2 = 0.
            dotProduct = 0.
            for fieldValueCount in fieldValueCounts:
                expected = fieldValueCount.attrib["count"]
                expectedNorm2 += expected**2

                bin = fieldValueCount.attrib["value"]
                if expected > 0. and bin in self.updators:
                    observed = self.updators[bin].sum()
                    dotProduct += expected * observed

            observedNorm2 = 0.
            for updator in self.updators.values():
                observed = updator.sum()
                observedNorm2 += observed**2

            if expectedNorm2 > 0. and observedNorm2 > 0.:
                if self.normalizationScheme is None:
                    return {SCORE_predictedValue: dotProduct}

                elif self.normalizationScheme is self.INDEPENDENT:
                    if expectedNorm2 <= 0. or observedNorm2 <= 0.: return INVALID
                    return {SCORE_predictedValue: dotProduct/math.sqrt(expectedNorm2)/math.sqrt(observedNorm2)}

                elif self.normalizationScheme is self.SIZEWEIGHTED:
                    if expectedNorm2 + observedNorm2 <= 0.: return INVALID
                    return {SCORE_predictedValue: 2.*dotProduct/(expectedNorm2 + observedNorm2)}

            else:
                return INVALID

    ######################################## chiSquareIndependence

    def _chiSquareIndependence_add(self, pmmlNode, fieldValues, totals):
        if isinstance(pmmlNode, (pmml.CountTable, pmml.NormalizedCountTable, pmml.FieldValue)):
            for child in pmmlNode:
                self._chiSquareIndependence_add(child, fieldValues + [child.attrib["value"]], totals)

        elif isinstance(pmmlNode, pmml.FieldValueCount):
            count = pmmlNode.attrib["count"]

            totals[None] += count
            for f, v in zip(self.fields, fieldValues):
                if v not in totals[f]:
                    totals[f][v] = 0.
                totals[f][v] += count

    def _chiSquareIndependence_chi2(self, pmmlNode, fieldValues, totals):
        if isinstance(pmmlNode, (pmml.CountTable, pmml.NormalizedCountTable, pmml.FieldValue)):
            output = 0.
            for child in pmmlNode:
                subchi2 = self._chiSquareIndependence_chi2(child, fieldValues + [child.attrib["value"]], totals)
                if subchi2 is None: return None
                output += subchi2
            return output

        elif isinstance(pmmlNode, pmml.FieldValueCount):
            observed = pmmlNode.attrib["count"]

            if totals[None] == 0:
                return None
            else:
                if isinstance(self.countTable, pmml.NormalizedCountTable):
                    scale = self.countTable.attrib["sample"]/totals[None]
                else:
                    scale = 1.

                expected = 1./(totals[None] * scale)**(len(self.fields) - 1)
                for f, v in zip(self.fields, fieldValues):
                    expected *= (totals[f][v] * scale)

                if expected == 0.:
                    return None
                else:
                    return (expected - (observed*scale))**2 / expected

    def scoreChiSquareIndependence(self, syncNumber, get):
        """Score one event with a chiSquareIndependence testStatistic.

        This reads from the multi-dimensional CountTable in PMML and
        ignores the data!  Data are only used to make the CountTable,
        so be sure to be running the producer if you want
        chiSquareIndependence.
        """

        # expect a CountTable (if it doesn't exist, the producer will make it)
        self.countTable = self.baseline.child()
        if not isinstance(self.countTable, (pmml.CountTable, pmml.NormalizedCountTable)):
            return INVALID   # the "first" time doesn't happen until we see a count table

        self.fields = []
        dimension = self.countTable.child(pmml.nonExtension)
        while True:
            self.fields.append(dimension.attrib["field"])
            if isinstance(dimension, pmml.FieldValueCount): break
            dimension = dimension.child(pmml.nonExtension)

        totals = {None: 0.}
        for f in self.fields:
            totals[f] = {}

        # every time: add up the n-field margins (which are "rows and columns" in 2-field case)
        self._chiSquareIndependence_add(self.countTable, [], totals)
        chi2 = self._chiSquareIndependence_chi2(self.countTable, [], totals)

        ndf = 1
        for f, tot in totals.items():
            if f is not None:
                ndf *= (len(tot) - 1)

        if chi2 is not None and ndf > 0:
            probability = chiSquare_cdf(chi2, ndf)
            pValue = 1. - probability
            return {SCORE_predictedValue: probability, SCORE_pValue: pValue, SCORE_chiSquare: chi2, SCORE_degreesOfFreedom: ndf}
        else:
            return INVALID

    ######################################## ODG-extension: GLR

    def _scoreGLR_GaussianDistribution(self, s, N):
        return (s - N*self.baseline.attrib["mean"])**2 / N

    def _scoreGLR_PoissonDistribution(self, s, N):
        if s > 0.:
            return -math.log(self.baseline.attrib["mean"])*s + math.log(s/N)*s + N*self.baseline.attrib["mean"] - s
        else:
            return -math.log(self.baseline.attrib["mean"])*s + N*self.baseline.attrib["mean"] - s

    def scoreGLR(self, syncNumber, get):
        """Score one event with a GLR testStatistic.

        Output is the *current* best-guess of the turn-around time (as
        the corresponding syncNumber) and its log-likelihood ratio.
        """

        # Eq. 2.4.40 in Basseville and Nikiforov: http://www.irisa.fr/sisthem/kniga/ (partly in eventweighting.py)

        value = get(self.field)
        if value is not INVALID and value is not MISSING:
            self.updator.increment(syncNumber, value)

        if isinstance(self.baseline, pmml.GaussianDistribution):
            maximum_syncNumber, maximum = self.updator.glr(self._scoreGLR_GaussianDistribution)

            if maximum is None or self.baseline.attrib["variance"] < 0.:
                return INVALID
            elif self.baseline.attrib["variance"] == 0.:
                return {SCORE_predictedValue: float("inf"), SCORE_thresholdTime: maximum_syncNumber}
            else:
                return {SCORE_predictedValue: maximum/2./self.baseline.attrib["variance"], SCORE_thresholdTime: maximum_syncNumber}

        elif isinstance(self.baseline, pmml.PoissonDistribution):
            maximum_syncNumber, maximum = self.updator.glr(self._scoreGLR_PoissonDistribution)

            if maximum is None:
                return INVALID
            else:
                return {SCORE_predictedValue: maximum, SCORE_thresholdTime: maximum_syncNumber}

#########################################################################################
######################################################################### producer ######
#########################################################################################

class ProducerBaselineModel(ProducerAlgorithm):
    defaultParams = {"updateExisting": "false"}

    def initialize(self, **params):
        """Initialize a baseline producer.

        Unlike other producers, this creates the update function
        dynamically, depending on the testStatistic.
        """

        testDistributions = self.segmentRecord.pmmlModel.child(pmml.TestDistributions)
        self.field = testDistributions.attrib["field"]

        if "updateExisting" in params:
            self.updateExisting = pmml.boolCheck(params["updateExisting"])
            del params["updateExisting"]
        else:
            self.updateExisting = pmml.boolCheck(self.defaultParams["updateExisting"])

        self.first = True

        testStatistic = self.segmentRecord.pmmlModel.child(pmml.TestDistributions).attrib["testStatistic"]
        if testStatistic in ("CUSUM", "zValue", "GLR"):
            self.baseline = testDistributions.child(pmml.Baseline).child()
            self.update = self.updateDistribution

            if testStatistic == "CUSUM":
                if "alternateField" in params:
                    self.alternateField = params["alternateField"]
                    del params["alternateField"]
                else:
                    self.alternateField = None

                if "alternateValue" in params:
                    self.alternateValue = params["alternateValue"]
                    del params["alternateValue"]
                else:
                    self.alternateValue = None

            else:
                self.alternateField = None
                self.alternateValue = None

        elif testStatistic in ("chiSquareDistribution", "scalarProduct"):
            self.weightField = testDistributions.attrib.get("weightField", None)
            self.countTable = testDistributions.child(pmml.Baseline).child(lambda x: isinstance(x, (pmml.CountTable, pmml.NormalizedCountTable)))
            self.update = self.updateHistogram

        elif testStatistic == "chiSquareIndependence": 
            self.baseline = testDistributions.child(pmml.Baseline)
            self.fields = None
            self.countTable = None

            self.updators = {}
            self.total_updator = self.engine.producerUpdateScheme.updator(SUMX)
            self.update = self.updateChiSquareIndependence

        if "alternateField" in params:
            raise NotImplementedError, "The 'alternateField' producerParameter is only used by CUSUM"
        if "alternateValue" in params:
            raise NotImplementedError, "The 'alternateValue' producerParameter is only used by CUSUM"

        if len(params) > 0:
            raise TypeError, "Unrecognized parameters %s" % params

    ######################################## CUSUM, zValue, GLR

    def _updateDistribution_first(self):
        if isinstance(self.baseline, (pmml.PoissonDistribution, pmml.GaussianDistribution)):
            self.baselinePartialSums = self.baseline.descendant(pmml.X_ODG_PartialSums, exception=False, maxdepth=2)
            if self.baselinePartialSums is None:
                self.baselinePartialSums = pmml.X_ODG_PartialSums()
                if not self.baseline.exists(pmml.Extension):
                    self.baseline.children.append(pmml.Extension())
                self.baseline.child(pmml.Extension).children.append(self.baselinePartialSums)

        if isinstance(self.baseline, pmml.PoissonDistribution):
            self.baselineUpdator = self.engine.producerUpdateScheme.updator(SUM1, SUMX)
            if self.updateExisting:
                self.baselineUpdator.initialize({COUNT: self.baselinePartialSums.attrib.get("COUNT", 0),
                                                 SUM1: self.baselinePartialSums.attrib.get("SUM1", 0.),
                                                 SUMX: self.baselinePartialSums.attrib.get("SUMX", 0.)})
            if COUNT in self.baselineUpdator.counters:
                self.baselinePartialSums.attrib["COUNT"] = self.baselineUpdator.counters[COUNT]
            self.baselinePartialSums.attrib["SUM1"] = self.baselineUpdator.counters[SUM1]
            self.baselinePartialSums.attrib["SUMX"] = self.baselineUpdator.counters[SUMX]

        elif isinstance(self.baseline, pmml.GaussianDistribution):
            self.baselineUpdator = self.engine.producerUpdateScheme.updator(SUM1, SUMX, SUMXX)
            if self.updateExisting:
                self.baselineUpdator.initialize({COUNT: self.baselinePartialSums.attrib.get("COUNT", 0),
                                                 SUM1: self.baselinePartialSums.attrib.get("SUM1", 0.),
                                                 SUMX: self.baselinePartialSums.attrib.get("SUMX", 0.),
                                                 SUMXX: self.baselinePartialSums.attrib.get("SUMXX", 0.)})
            if COUNT in self.baselineUpdator.counters:
                self.baselinePartialSums.attrib["COUNT"] = self.baselineUpdator.counters[COUNT]
            self.baselinePartialSums.attrib["SUM1"] = self.baselineUpdator.counters[SUM1]
            self.baselinePartialSums.attrib["SUMX"] = self.baselineUpdator.counters[SUMX]
            self.baselinePartialSums.attrib["SUMXX"] = self.baselineUpdator.counters[SUMXX]

        elif isinstance(self.baseline, pmml.UniformDistribution):
            self.baselineUpdator = self.engine.producerUpdateScheme.updator(MIN, MAX)
            if self.updateExisting:
                self.baselineUpdator.initialize({MIN: self.baseline.attrib["lower"], MAX: self.baseline.attrib["upper"]})

        else:
            raise NotImplementedError, "Only production of Gaussian, Poisson, and Uniform distributions has been implemented."

        if self.alternateField is not None:
            if not testDistributions.exists(pmml.Alternate):
                raise RuntimeError, "alternateField requested but there is no <Alternate/> distribution in the PMML"

            self.alternate = testDistributions.child(pmml.Alternate).child()

            if isinstance(self.alternate, (pmml.PoissonDistribution, pmml.GaussianDistribution)):
                self.alternatePartialSums = self.alternate.descendant(pmml.X_ODG_PartialSums, exception=False, maxdepth=2)
                if self.alternatePartialSums is None:
                    self.alternatePartialSums = pmml.X_ODG_PartialSums()
                    if not self.alternate.exists(pmml.Extension):
                        self.alternate.children.append(pmml.Extension())
                    self.alternate.child(pmml.Extension).children.append(self.alternatePartialSums)

            if isinstance(self.alternate, pmml.PoissonDistribution):
                self.alternateUpdator = self.engine.producerUpdateScheme.updator(SUM1, SUMX)
                if self.updateExisting:
                    self.alternateUpdator.initialize({COUNT: self.alternatePartialSums.attrib.get("COUNT", 0),
                                                      SUM1: self.alternatePartialSums.attrib.get("SUM1", 0.),
                                                      SUMX: self.alternatePartialSums.attrib.get("SUMX", 0.)})
                if COUNT in self.alternateUpdator.counters:
                    self.alternatePartialSums.attrib["COUNT"] = self.alternateUpdator.counters[COUNT]
                self.alternatePartialSums.attrib["SUM1"] = self.alternateUpdator.counters[SUM1]
                self.alternatePartialSums.attrib["SUMX"] = self.alternateUpdator.counters[SUMX]

            elif isinstance(self.alternate, pmml.GaussianDistribution):
                self.alternateUpdator = self.engine.producerUpdateScheme.updator(SUM1, SUMX, SUMXX)
                if self.updateExisting:
                    self.alternateUpdator.initialize({COUNT: self.alternatePartialSums.attrib.get("COUNT", 0),
                                                      SUM1: self.alternatePartialSums.attrib.get("SUM1", 0.),
                                                      SUMX: self.alternatePartialSums.attrib.get("SUMX", 0.),
                                                      SUMXX: self.alternatePartialSums.attrib.get("SUMXX", 0.)})
                if COUNT in self.alternateUpdator.counters:
                    self.alternatePartialSums.attrib["COUNT"] = self.alternateUpdator.counters[COUNT]
                self.alternatePartialSums.attrib["SUM1"] = self.alternateUpdator.counters[SUM1]
                self.alternatePartialSums.attrib["SUMX"] = self.alternateUpdator.counters[SUMX]
                self.alternatePartialSums.attrib["SUMXX"] = self.alternateUpdator.counters[SUMXX]

            elif isinstance(self.alternate, pmml.UniformDistribution):
                self.alternateUpdator = self.engine.producerUpdateScheme.updator(MIN, MAX)
                if self.updateExisting:
                    self.alteranteUpdator.initialize({MIN: self.alterante.attrib["lower"], MAX: self.alterante.attrib["upper"]})

            else:
                raise NotImplementedError, "Only production of Gaussian, Poisson, and Uniform distributions has been implemented."

        else:
            self.alternate = None

    def updateDistribution(self, syncNumber, get):
        """Update a baseline model with a CUSUM, zValue, or GLR
        testStatistic (parameterized distribution)."""

        if self.first:
            self._updateDistribution_first()
            self.first = False

        value = get(self.field)
        if value is INVALID or value is MISSING: return False

        distribution = self.baseline
        updator = self.baselineUpdator
        partialSums = self.baselinePartialSums
        
        if self.alternate is not None:
            alternateValue = get(self.alternateField)
            if alternateValue is INVALID or alternateValue is MISSING: return False

            if alternateValue == self.alternateValue:
                distribution = self.alternate
                updator = self.alternateUpdator
                partialSums = self.alternatePartialSums

        updator.increment(syncNumber, value)

        if isinstance(self.baseline, pmml.PoissonDistribution):
            mean = updator.mean()
            if mean is INVALID: return False
            # FIXME: Poisson distributions should never have zero or negative means.
            # What should we do if the mean is not positive?  Return False?
            # Only increment the updator if value is positive (DANGEROUS!)  Something else?
            distribution.attrib["mean"] = mean
            if COUNT in updator.counters:
                partialSums.attrib["COUNT"] = updator.counters[COUNT]
            partialSums.attrib["SUM1"] = updator.counters[SUM1]
            partialSums.attrib["SUMX"] = updator.counters[SUMX]

        elif isinstance(self.baseline, pmml.GaussianDistribution):
            mean = updator.mean()
            variance = updator.variance()
            if mean is INVALID or variance is INVALID: return False
            distribution.attrib["mean"] = mean
            distribution.attrib["variance"] = variance
            if COUNT in updator.counters:
                partialSums.attrib["COUNT"] = updator.counters[COUNT]
            partialSums.attrib["SUM1"] = updator.counters[SUM1]
            partialSums.attrib["SUMX"] = updator.counters[SUMX]
            partialSums.attrib["SUMXX"] = updator.counters[SUMXX]

        elif isinstance(self.baseline, pmml.UniformDistribution):
            _min = updator.min()
            _max = updator.max()
            if _min is INVALID or _max is INVALID: return False
            distribution.attrib["lower"] = _min
            distribution.attrib["upper"] = _max

        else:
            raise NotImplementedError, "Only the Poisson and Gaussian baseline producers have been implemented."

        return True

    ######################################## chiSquareDistribution and scalarProduct

    def _updateHistogram_first(self):
        if self.updateExisting:
            if "sample" not in self.countTable.attrib and isinstance(self.countTable, pmml.CountTable):
                self.countTable.attrib["sample"] = 0.
                for fieldValueCount in self.countTable.matches(pmml.FieldValueCount, maxdepth=None):
                    self.countTable.attrib["sample"] += fieldValueCount.attrib["count"]
        else:
            self.countTable.attrib["sample"] = 0.

        self.total_updator = self.engine.producerUpdateScheme.updator(SUMX)
        self.total_updator.initialize({SUMX: self.countTable.attrib["sample"]})

        self.pmmlEntries = {}
        self.updators = {}
        for child in self.countTable.matches(pmml.nonExtension):
            value = child.attrib["value"]
            self.pmmlEntries[value] = child
            self.updators[value] = self.engine.producerUpdateScheme.updator(SUMX)
            if self.updateExisting:
                self.updators[value].initialize({SUMX: child.attrib["count"]})
            else:
                child.attrib["count"] = self.updators[value].sum()

    def updateHistogram(self, syncNumber, get):
        """Update a baseline model with a chiSquareDistribution or
        scalarProduct testStatistic (binned histogram)."""

        if self.first:
            self._updateHistogram_first()
            self.first = False

        value = get(self.field)
        if value is INVALID or value is MISSING: return False

        if self.weightField is None:
            weight = 1.
        else:
            weight = get(self.weightField)
            if weight is INVALID or weight is MISSING: return False

        # this might be a new bin
        if value not in self.pmmlEntries:
            newNode = pmml.FieldValueCount(field=self.field, value=value, count=0)    # FIXME: should field=self.field???
            self.countTable.children.append(newNode)
            self.pmmlEntries[value] = newNode
            self.updators[value] = self.engine.producerUpdateScheme.updator(SUMX)

        # for histograms, increment all bins, but only the correct bin gets a non-zero value
        for bin, updator in self.updators.items():
            if bin == value:
                updator.increment(syncNumber, weight)
            else:
                updator.increment(syncNumber, 0.)
            self.pmmlEntries[bin].attrib["count"] = self.updators[bin].sum()

        self.total_updator.increment(syncNumber, weight)
        self.countTable.attrib["sample"] = self.total_updator.sum()

        return True

    ######################################## chiSquareIndependence

    def _updateChiSquareIndependence_first(self, get):
        # get self.fields from a list of FieldRefs
        if isinstance(self.baseline.child(), pmml.FieldRef):
            self.fields = [child.attrib["field"] for child in self.baseline]

            got = {}
            for f in self.fields:
                value = get(f)
                if value is INVALID or value is MISSING:
                    self.fields = None
                    return False
                got[f] = value

            self._updateChiSquareIndependence_newTableFromFields(got)

        # get self.fields from a NormalizedCountTable???
        elif isinstance(self.baseline.child(), pmml.NormalizedCountTable):
            raise NotImplementedError, "Model updating, starting from a NormalizedCountTable, hasn't been implemented"

        # get self.fields from a pre-existing CountTable
        else:
            self.fields = []
            self.countTable = self.baseline.child()
            dimension = self.countTable.child(pmml.nonExtension)
            while True:
                self.fields.append(dimension.attrib["field"])
                if isinstance(dimension, pmml.FieldValueCount): break
                dimension = dimension.child(pmml.nonExtension)

            if self.updateExisting:
                if "sample" not in self.countTable.attrib and isinstance(self.countTable, pmml.CountTable):
                    self.countTable.attrib["sample"] = 0.
                    for fieldValueCount in self.countTable.matches(pmml.FieldValueCount, maxdepth=None):
                        self.countTable.attrib["sample"] += fieldValueCount.attrib["count"]

                self.total_updator.initialize({SUMX: self.countTable.attrib["sample"]})

            else:
                self._updateChiSquareIndependence_newTableFromFields(got)

        return True

    def _updateChiSquareIndependence_findNode(self, depth, got, node):
        field = self.fields[depth]
        value = got[field]
        for child in node:
            if child.attrib["value"] == value:
                if depth == len(self.fields) - 1:
                    return depth + 1, child
                else:
                    return self._updateChiSquareIndependence_findNode(depth + 1, got, child)
        return depth, node

    def _updateChiSquareIndependence_newTableFromFields(self, got):
        self.countTable = pmml.CountTable(sample=0.)
        dimension = self.countTable
        for i, f in enumerate(self.fields):
            if i == len(self.fields) - 1:
                dimension.children.append(pmml.FieldValueCount(field=f, value=got[f], count=0.))
            else:
                dimension.children.append(pmml.FieldValue(field=f, value=got[f]))
            dimension = dimension.child(pmml.nonExtension)

        self.baseline.children = [self.countTable]

    def updateChiSquareIndependence(self, syncNumber, get):
        """Update a baseline model with a chiSquareIndependence
        testStatistic.

        This updates a multi-dimensional CountTable with the data,
        entirely encoded in PMML.
        """

        if self.first:
            if not self._updateChiSquareIndependence_first(get):
                return False
            self.first = False

        got = {}
        key = []
        for f in self.fields:
            value = get(f)
            if value is INVALID or value is MISSING: return False
            got[f] = value
            key.append(value)
        key = tuple(key)

        depth, node = self._updateChiSquareIndependence_findNode(0, got, self.countTable)

        # if we don't have a table entry for this yet, make one
        for i in xrange(depth, len(self.fields)):
            f = self.fields[i]
            if i == len(self.fields) - 1:
                child = pmml.FieldValueCount(field=f, value=got[f], count=0.)
            else:
                child = pmml.FieldValue(field=f, value=got[f])

            node.children.append(child)
            node = child

        updator = self.updators.get(key, None)
        if updator is None:
            updator = self.engine.producerUpdateScheme.updator(SUMX)
            updator.initialize({SUMX: node.attrib["count"]})
            self.updators[key] = updator

        # for histograms, increment all bins, but only the correct bin gets a non-zero value
        for otherupdator in self.updators.values():
            if updator is not otherupdator:
                otherupdator.increment(syncNumber, 0.)

        updator.increment(syncNumber, 1.)
        node.attrib["count"] = updator.sum()

        self.total_updator.increment(syncNumber, 1.)
        self.countTable.attrib["sample"] = self.total_updator.sum()

        return True
