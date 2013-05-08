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

"""Defines the way all consumer algorithms store their states, and how
events are weighted or blended.  Can be expanded to handle model
production in a parallelized system."""

import numpy
import numpy.linalg

from augustus.core.defs import Atom, INVALID
from augustus.core.extramath import MINFLOAT

########################################################### Atoms

COUNT = Atom("Count")
SUM1 = Atom("Sum1")
SUMX = Atom("SumX")
SUMXX = Atom("SumXX")
RUNMEAN = Atom("RunMean")
RUNSN = Atom("RunSN")
MIN = Atom("Min")
MAX = Atom("Max")
CUSUM = Atom("CUSUM")
GLR = Atom("GLR")

class COVARIANCE(Atom):
    """Atom (isotope?) for covariance calculations.  The dimension of
    this object depends on the data, and is given at initialization."""

    def __init__(self, dimension):
        self.name = "Covariance"
        self.dimension = dimension
        self.hash = hash("Covariance")

    def __repr__(self):
        return "COVARIANCE-%d" % self.dimension

    def __str__(self):
        return "Covariance-%d" % self.dimension

    def __eq__(self, other):
        return isinstance(other, COVARIANCE)

COVARIANCE0 = COVARIANCE(0)

class OLS(Atom):
    """Atom (isotope?) for ordinary least-squares calculations.  The dimension of
    this object depends on the data, and is given at initialization."""

    def __init__(self, p):
        self.name = "OLS"
        self.p = p
        self.hash = hash("OLS")

    def __repr__(self):
        return "OLS-%d" % self.p

    def __str__(self):
        return "OLS-%d" % self.p

    def __eq__(self, other):
        return isinstance(other, OLS)

OLS0 = OLS(0)

########################################################### Calculation helpers

### TODO: make a calculation helper for covariance, too.

class OrdinaryLeastSquares(object):
    """Computes OLS as a running sum.  The memory usage scales only
    with p, the number of parameters, not n, the number of observed
    data points, and the hardest computation (in the estimator method)
    is a pxp matrix inversion."""

    def __init__(self, p):
        self.n = 0.
        self.p = p
        self.matrix = numpy.matrix(numpy.zeros((p, p), dtype=numpy.double))
        self.vector = numpy.matrix(numpy.zeros((p, 1), dtype=numpy.double))

    def increment(self, values, alpha=0., weight=1.):
        # values[0] is the dependent variable
        # values[1:p+1] are the independent variables

        # add to the outer product matrix
        for j in xrange(1, len(values)):
            for k in xrange(1, len(values)):
                self.matrix[j-1, k-1] = weight*(values[j] * values[k]) + (1. - alpha)*self.matrix[j-1, k-1]

        # add to the vector
        for j in xrange(1, len(values)):
            self.vector[j-1, 0] = weight*(values[j] * values[0]) + (1. - alpha)*self.vector[j-1, 0]

        # add to n
        self.n = weight + (1. - alpha) * self.n

    def estimator(self):
        # returns a p-length list of estimators beta to optimize
        #      dependent_i = independent_i . beta + hidden_i
        # by minimizing hidden_i over incrementations i
        if self.n < 1.:
            return INVALID

        try:
            return map(float, numpy.dot(numpy.linalg.inv(self.matrix / self.n), (self.vector / self.n)))
        except numpy.linalg.LinAlgError:
            return INVALID

########################################################### UpdateScheme and Updators

class Updator(object):
    """Basic unweighted updator.

    In most cases (e.g. COUNT, SUM1, SUMX), this is just a number that
    gets incremented.  (Exceptions are GLR, which must be a list of
    events and COVARIANCE, which is a tensor.)"""

    requiredParams = []

    def __init__(self, counters, params):
        """Create an Updator with a list of 'counters' Atoms.

        The 'params' depend on the event-weighting scheme (none for
        unweighted).

        Should only ever be called by UpdateScheme.updator(counters).

        Calls clear() to initialize the counters to zero (or empty
        set, or whatever).
        """

        if (RUNSN in counters) and RUNMEAN not in counters:
            counters = counters + (RUNMEAN,)

        if (RUNSN in counters) and COUNT not in counters:
            counters = counters + (COUNT,)

        for p in self.requiredParams:
            if p not in params:
                raise TypeError("Required parameter \"%s\" for %s is missing" % (p, self.__class__.__name__))
        self.__dict__.update(params)

        self.counters = dict([(c, None) for c in counters])
        self.clear()

    def initialize(self, values):
        """Initialize the counters to a specific value.

        The 'values' are given as a dictionary of {COUNTER_ATOM:
        value} pairs.
        """

        for key, value in values.items():
            if key in self.counters:
                self.counters[key] = value

    def increment(self, syncNumber, value):
        """Increment the counter.  What this does depends on what kind
        of counters are included in the updator."""

        if COUNT in self.counters:
            self.counters[COUNT] += 1

        if SUM1 in self.counters:
            self.counters[SUM1] += 1.

        if SUMX in self.counters:
            self.counters[SUMX] += value
            
        if SUMXX in self.counters:
            self.counters[SUMXX] += value**2

        if RUNMEAN in self.counters:
            oldRunMean = self.counters[RUNMEAN]
            self.counters[RUNMEAN] += (value - oldRunMean)/self.counters[COUNT]

            if RUNSN in self.counters:
                self.counters[RUNSN] += (value - oldRunMean)*(value - self.counters[RUNMEAN])

        if MIN in self.counters:
            if self.counters[MIN] is None or self.counters[MIN] > value:
                self.counters[MIN] = value

        if MAX in self.counters:
            if self.counters[MAX] is None or self.counters[MAX] < value:
                self.counters[MAX] = value

        if CUSUM in self.counters:
            cusum = self.counters[CUSUM]
            if len(cusum) != 1: cusum.append(0.)
            cusum[0] = max(self.resetValue, cusum[0] + value)

        if GLR in self.counters:
            self.counters[GLR].append((syncNumber, value))

        c = self.counters.get(COVARIANCE0, None)
        if c is not None:
            c[0] += 1.
            k = 1
            for i in xrange(len(value)):
                c[k] += value[i]
                k += 1
            for i in xrange(len(value)):
                for j in xrange(i, len(value)):
                    c[k] += value[i] * value[j]
                    k += 1

        ols = self.counters.get(OLS0, None)
        if ols is not None:
            ols.increment(value)

    def covarianceKey(self):
        """Assuming that the updator contains only one covariance,
        return the key for that counter.

        Only used internally by Updator."""

        key = None
        for c in self.counters:
            if isinstance(c, COVARIANCE):
                key = c
                break
        return key

    def olsKey(self):
        """Assuming that the updator contains only one ols,
        return the key for that counter.

        Only used internally by Updator."""

        key = None
        for c in self.counters:
            if isinstance(c, OLS):
                key = c
                break
        return key

    def clear(self):
        """Initialize all existent counters to zero, or empty set, or
        whatever."""

        if COUNT in self.counters: self.counters[COUNT] = 0
        if SUM1 in self.counters: self.counters[SUM1] = 0.
        if SUMX in self.counters: self.counters[SUMX] = 0.
        if SUMXX in self.counters: self.counters[SUMXX] = 0.
        if RUNMEAN in self.counters:
            self.counters[RUNMEAN] = 0.
            if RUNSN in self.counters: self.counters[RUNSN] = 0.
        if MIN in self.counters: self.counters[MIN] = None
        if MAX in self.counters: self.counters[MAX] = None
        if CUSUM in self.counters: self.counters[CUSUM] = []
        if GLR in self.counters: self.counters[GLR] = []

        c = self.covarianceKey()
        if c is not None:
            self.counters[COVARIANCE0] = numpy.zeros((1 + c.dimension + (c.dimension * (c.dimension + 1) / 2)), dtype=numpy.float)
            self.counters[COVARIANCE0][0] = -1.

        ols = self.olsKey()
        if ols is not None:
            self.counters[ols] = OrdinaryLeastSquares(ols.p)

    def count(self):
        """Return the current count.  Requires a COUNT counter
        (doesn't check!)"""

        return self.counters[COUNT]

    def mean(self):
        """Return the current mean.  Requires SUM1 and SUMX counters
        (doesn't check!)"""

        if self.counters[SUM1] > 0.:
            return self.counters[SUMX] / self.counters[SUM1]
        else:
            return INVALID

    def variance(self):
        """Return the current variance.  Requires SUM1, SUMX, and
        SUMXX counters (doesn't check!)"""

        if self.counters[SUM1] > 1.:
            meansquared = (self.counters[SUMX] / self.counters[SUM1])**2
            sumxxsquared = self.counters[SUMXX] / self.counters[SUM1]
            difference = sumxxsquared - meansquared

            if meansquared == 0. or sumxxsquared == 0. or \
                   min(abs(difference / meansquared), abs(difference / sumxxsquared)) < MINFLOAT:
                return 0.0
            else:
                return (self.counters[SUM1] / (self.counters[SUM1] - 1)) * difference

        else:
            return INVALID

    def runMean(self):
        """Return the mean using a running-sum algorithm.  Requires
        RUNMEAN (doesn't check!)"""

        return self.counters[RUNMEAN]

    def runVariance(self):
        """Return the variance using a running-sum algorithm.
        Requires RUNSN and COUNT (doesn't check!)"""

        if self.counters[COUNT] <= 1:
            return INVALID
        else:
            return self.counters[RUNSN] / (self.counters[COUNT] - 1)

    def sum(self):
        """Return the current sum.  Requires a SUMX counter (doesn't
        check!)"""

        return self.counters[SUMX]

    def min(self):
        """Return the current minimum.  Requires a MIN counter
        (doesn't check!)"""

        output = self.counters[MIN]
        if output is None:
            return INVALID
        else:
            return output

    def max(self):
        """Return the current maximum.  Requires a MAX counter
        (doesn't check!)"""

        output = self.counters[MAX]
        if output is None:
            return INVALID
        else:
            return output

    def cusum(self):
        """Return the current cusum.  Requires a CUSUM counter
        (doesn't check!)"""

        try:
            return self.counters[CUSUM][0]
        except IndexError:
            return INVALID

    def glr(self, f):
        """Return the current GLR, using a function f that partially
        calculates the GLR for this case (Gaussian, Poisson, etc.).
        Requires a GLR counter (doesn't check!)"""

        # Eq. 2.4.40 in Basseville and Nikiforov: http://www.irisa.fr/sisthem/kniga/ (partly in baseline.py)
        glrlist = self.counters[GLR]

        maximum = None
        maximum_syncNumber = None
        last = 0.
        denominator = 0.
        for j in xrange(len(glrlist) - 1, -1, -1):
            syncNumber, trial = glrlist[j]
            trial += last
            last = trial
            denominator += 1.

            trial = f(trial, denominator)

            if maximum is None or trial > maximum:
                maximum = trial
                maximum_syncNumber = syncNumber

        return maximum_syncNumber, maximum

    def _cov(self, key, numbers, i, j):
        if i > j:
            i, j = j, i
        sum1 = numbers[0]
        sumx = numbers[1 + i]
        sumy = numbers[1 + j]
        sumxy = numbers[1 + key.dimension + i*key.dimension - (i-1)*((i-1) + 1)/2 + j - i]
        return sumxy/sum1 - (sumx/sum1)*(sumy/sum1)

    def covariance(self):
        """Return the current covariance matrix as a NumPy matrix.
        Requires a COVARIANCE(N) counter (doesn't check!)"""

        key = self.covarianceKey()
        numbers = self.counters[key]
        if numbers[0] <= 0.:
            return INVALID
        return numpy.matrix([[self._cov(key, numbers, i, j) for i in xrange(key.dimension)] for j in xrange(key.dimension)])

    def covmean(self):
        """Return the current mean vector as a NumPy vector.
        Requires a COVARIANCE(N) counter (doesn't check!)"""

        key = self.covarianceKey()
        numbers = self.counters[key]
        if numbers[0] <= 0.:
            return INVALID
        sum1 = numbers[0]
        return numpy.matrix([numbers[1 + i]/sum1 for i in xrange(key.dimension)]).T

    def ordinaryLeastSquares(self):
        """Return a list of estimated parameters obtained by ordinary
        least squares regression for the data accumulated so far."""

        return self.counters[OLS0].estimator()

class UpdatorExponential(Updator):
    """Exponentially weighted updator.

    As events recede into the past, their counters are repeatedly
    multiplied by alpha to suppress their significance in
    calculations."""

    requiredParams = ["alpha"]

    def __init__(self, counters, params):
        """Create an Updator with a list of 'counters' Atoms.

        For mean and variance calculations (including covariance),
        COUNT is added to the set of counters because we'll need that
        for a correction.

        CUSUM, MIN, and MAX are not implemented.

        Calls Updator.__init__().
        """

        if (SUM1 in counters or COVARIANCE0 in counters) and COUNT not in counters:
            counters = counters + (COUNT,)

        if CUSUM in counters:
            raise NotImplementedError("Exponential weighting has not been implemented for CUSUM")

        if MIN in counters or MAX in counters:
            raise NotImplementedError("Exponential weighting doesn't make sense for MIN/MAX")

        Updator.__init__(self, counters, params)

    def increment(self, syncNumber, value):
        """Increment the counter, multiplying all previous events by alpha."""
        
        if COUNT in self.counters:
            self.counters[COUNT] += 1

        if SUMX in self.counters:
            self.counters[SUMX] = value + (1. - self.alpha) * self.counters[SUMX]

        if SUMXX in self.counters:
            self.counters[SUMXX] = value**2 + (1. - self.alpha) * self.counters[SUMXX]

        if SUM1 in self.counters:
            self.counters[SUM1] = 1. + (1. - self.alpha) * self.counters[SUM1]

            if self.counters[COUNT] == 1:
                if SUMX in self.counters:
                    self.counters[SUMX] = value/self.alpha

                if SUMXX in self.counters:
                    self.counters[SUMXX] = value**2/self.alpha

        if RUNMEAN in self.counters:
            oldRunMean = self.counters[RUNMEAN]
            diff = (value - oldRunMean)
            incr = self.alpha * diff
            self.counters[RUNMEAN] += incr

            if RUNSN in self.counters:
                self.counters[RUNSN] = (1. - self.alpha)*(self.counters[RUNSN] + diff*incr)

        if GLR in self.counters:
            self.counters[GLR].append((syncNumber, value))

        c = self.counters.get(COVARIANCE0, None)
        if c is not None:
            c[0] = 1. + (1. - self.alpha) * c[0]
            k = 1
            for i in xrange(len(value)):
                c[k] = value[i] + (1. - self.alpha) * c[k]
                k += 1
            for i in xrange(len(value)):
                for j in xrange(i, len(value)):
                    c[k] = value[i] * value[j] + (1. - self.alpha) * c[k]
                    k += 1

        ols = self.counters.get(OLS0, None)
        if ols is not None:
            ols.increment(value, self.alpha)

    def mean(self):
        """Return the current mean.  Requires SUM1, SUMX, and COUNT
        counters (doesn't check!)

        Applies a correction to bring this calculation in line with
        the way statisticians usually think of 'alpha' in exponential
        weighting.
        """

        if self.counters[SUM1] > 0.:
            return self.counters[SUMX] * (1. - (1. - self.alpha)**self.counters[COUNT]) / self.counters[SUM1]
        else:
            return INVALID

    def variance(self):
        """Return the current mean.  Requires SUM1, SUMX, SUMXX, and
        COUNT counters (doesn't check!)

        Applies a correction to bring this calculation in line with
        the way statisticians usually think of 'alpha' in exponential
        weighting.
        """

        if self.counters[COUNT] > 1:
            n = self.counters[COUNT]
            meansquared = self.mean()**2
            sumxxsquared = self.counters[SUMXX] * (1. - (1. - self.alpha)**n) / self.counters[SUM1]
            difference = sumxxsquared - meansquared
            if meansquared == 0. or sumxxsquared == 0. or \
                   min(abs(difference / meansquared), abs(difference / sumxxsquared)) < MINFLOAT:
                return 0.0
            else:
                return (n / (n - 1.)) * difference

        else:
            return INVALID

    def runVariance(self):
        """Return the variance using a running-sum algorithm.
        Requires RUNSN and COUNT (doesn't check!)"""

        if self.counters[COUNT] <= 1:
            return INVALID
        else:
            return self.counters[RUNSN] * self.counters[COUNT] / (self.counters[COUNT] - 1.)

    def glr(self, f):
        """Return the current GLR, using a function f that partially
        calculates the GLR for this case (Gaussian, Poisson, etc.).
        Requires a GLR counter (doesn't check!)"""

        # Eq. 2.4.40 in Basseville and Nikiforov: http://www.irisa.fr/sisthem/kniga/ (partly in baseline.py)
        glrlist = self.counters[GLR]

        maximum = None
        maximum_syncNumber = None
        last = 0.
        denominator = 0.
        for j in xrange(len(glrlist) - 1, -1, -1):
            syncNumber, trial = glrlist[j]
            trial += last*(1. - self.alpha)
            last = trial
            denominator = denominator*(1. - self.alpha) + 1.

            trial = f(trial, denominator)

            if maximum is None or trial > maximum:
                maximum = trial
                maximum_syncNumber = syncNumber

        return maximum_syncNumber, maximum

    def _cov(self, key, numbers, i, j):
        if i > j:
            i, j = j, i

        sum1 = numbers[0]
        sumx = numbers[1 + i]
        sumy = numbers[1 + j]
        sumxy = numbers[1 + key.dimension + i*key.dimension - (i-1)*((i-1) + 1)/2 + j - i]

        n = self.counters[COUNT]
        meanx = sumx * (1. - (1. - self.alpha)**(n + 1)) / sum1
        meany = sumy * (1. - (1. - self.alpha)**(n + 1)) / sum1
        return (n / (n - 1)) * ((sumxy * (1. - (1. - self.alpha)**(n + 1)) / sum1) - meanx*meany)

    def covmean(self):
        """Return the current mean vector as a NumPy vector.
        Requires a COVARIANCE(N) counter (doesn't check!)

        Applies the same correction as in mean().
        """

        key = self.covarianceKey()
        numbers = self.counters[key]
        if numbers[0] <= 0.:
            return INVALID
        sum1 = numbers[0]
        return numpy.matrix([numbers[1 + i] * (1. - (1. - self.alpha)**(self.counters[COUNT] + 1)) / sum1 for i in xrange(key.dimension)]).T

class UpdatorWindow(Updator):
    """Window-based updator.

    Only events within a given window are relevant; all others
    effectively have zero weight.

    The memory usage of this Updator grows with windowSize +
    windowLag, since all events must be stored as a list until they
    become irrelevant.

    CPU time should not be severely affected, since events are only
    pushed or popped from the ends of the list--- no
    O(windowSize+windowLag) operations are performed for each event.
    """

    requiredParams = ["windowSize", "windowLag"]

    def __init__(self, counters, params):
        """Create an Updator with a list of 'counters' Atoms.

        Creates the event history and calls Updator.__init__().
        """

        # It's less necessary to do running means and variances when the size of the dataset is limited
        if RUNMEAN in counters:
            c = set(counters)
            c.discard(RUNMEAN)
            c.add(SUM1)
            c.add(SUMX)
            counters = tuple(c)
        if RUNSN in counters:
            c = set(counters)
            c.discard(RUNSN)
            c.add(SUM1)
            c.add(SUMX)
            c.add(SUMXX)
            counters = tuple(c)

        Updator.__init__(self, counters, params)
        self.history = []

        if self.windowSize < 0:
            raise ValueError("UpdatorWindow windowSize must be non-negative")
        if self.windowLag < 0:
            raise ValueError("UpdatorWindow windowLag must be non-negative")

    def runMean(self):
        return self.mean()

    def runVariance(self):
        return self.variance()

    def increment(self, syncNumber, value):
        """Increment the counter, pushing a value onto one end of
        the event history and popping another off the other end.

        For most counters, we simply add the new event and subtract
        the old event to maintain a consistent count.
        """

        # put this new value at the beginning of the history list
        self.history.insert(0, (syncNumber, value))

        # a value at self.history[self.windowLag] will flow into the dataset
        if self.windowLag < len(self.history):
            inboxSync, inboxValue = self.history[self.windowLag]
        else:
            inboxSync, inboxValue = None, None

        # a value at self.history[self.windowLag + self.windowSize] will flow out of the dataset
        # also, pop it from the history
        if len(self.history) > self.windowLag + self.windowSize:
            outboxSync, outboxValue = self.history.pop()
        else:
            outboxSync, outboxValue = None, None
        
        if inboxValue is not None:
            if COUNT in self.counters:
                self.counters[COUNT] += 1

            if SUM1 in self.counters:
                self.counters[SUM1] += 1.

            if SUMX in self.counters:
                self.counters[SUMX] += inboxValue

            if SUMXX in self.counters:
                self.counters[SUMXX] += inboxValue**2

            if CUSUM in self.counters:
                cusum = self.counters[CUSUM]
                cusum.append(0.)
                for i in xrange(len(cusum)):
                    cusum[i] = max(self.resetValue, cusum[i] + inboxValue)

            if GLR in self.counters:
                self.counters[GLR].append((inboxSync, inboxValue))

            c = self.counters.get(COVARIANCE0, None)
            if c is not None:
                c[0] += 1.
                k = 1
                for i in xrange(len(value)):
                    c[k] += inboxValue[i]
                    k += 1
                for i in xrange(len(value)):
                    for j in xrange(i, len(value)):
                        c[k] += inboxValue[i] * inboxValue[j]
                        k += 1

            ols = self.counters.get(OLS0, None)
            if ols is not None:
                ols.increment(inboxValue)

        if outboxValue is not None:
            if COUNT in self.counters:
                self.counters[COUNT] -= 1

            if SUM1 in self.counters:
                self.counters[SUM1] -= 1.

            if SUMX in self.counters:
                self.counters[SUMX] -= outboxValue

            if SUMXX in self.counters:
                self.counters[SUMXX] -= outboxValue**2

            if CUSUM in self.counters:
                self.counters[CUSUM] = self.counters[CUSUM][1:]

            if GLR in self.counters:
                self.counters[GLR] = self.counters[GLR][1:]

            c = self.counters.get(COVARIANCE0, None)
            if c is not None:
                c[0] -= 1.
                k = 1
                for i in xrange(len(value)):
                    c[k] -= outboxValue[i]
                    k += 1
                for i in xrange(len(value)):
                    for j in xrange(i, len(value)):
                        c[k] -= outboxValue[i] * outboxValue[j]
                        k += 1

            ols = self.counters.get(OLS0, None)
            if ols is not None:
                ols.increment(inboxValue, weight=-1.)

        # for min, max, and set, you have to re-evaluate all data in the box each time
        if MIN in self.counters or MAX in self.counters:
            window = [v for s, v in self.history[self.windowLag:(self.windowLag + self.windowSize)]]

            if MIN in self.counters:
                if len(window) == 0: self.counters[MIN] = None
                else: self.counters[MIN] = min(window)

            if MAX in self.counters:
                if len(window) == 0: self.counters[MAX] = None
                else: self.counters[MAX] = max(window)

class UpdatorSynchronized(Updator):
    """Window-based updator, synchronized by syncNumber.

    This differs from a simple window in that everything with the same
    syncNumber (usually the global event number) is either in the
    window or out of the window, *across all possible updators*.

    Thus, even if a segment doesn't match, all updators within the
    segment may lose an event off the end of the window because it's
    time for that event to go away.

    Also, syncNumbers don't need to be sequential (updators in
    non-matching segments wouldn't be able to tell, anyway).

    This comes at a severe CPU cost, since everything in the event
    history list must be reevaluated for every event.  That is, CPU
    time scales as O(events)*O(windowSize+windowLag).  But it is
    useful for some applications.
    """

    requiredParams = ["windowSize", "windowLag"]

    def __init__(self, counters, params):
        """Create an Updator with a list of 'counters' Atoms.

        Creates the event history and calls Updator.__init__().
        """

        # It's less necessary to do running means and variances when the size of the dataset is limited
        if RUNMEAN in counters:
            c = set(counters)
            c.discard(RUNMEAN)
            c.add(SUM1)
            c.add(SUMX)
            counters = tuple(c)
        if RUNSN in counters:
            c = set(counters)
            c.discard(RUNSN)
            c.add(SUM1)
            c.add(SUMX)
            c.add(SUMXX)
            counters = tuple(c)

        Updator.__init__(self, counters, params)
        self.history = []

        if self.windowSize < 0:
            raise ValueError("UpdatorSynchronized windowSize must be non-negative")
        if self.windowLag < 0:
            raise ValueError("UpdatorSynchronized windowLag must be non-negative")

    def runMean(self):
        return self.mean()

    def runVariance(self):
        return self.variance()

    def increment(self, syncNumber, value):
        """Increment the counter, pushing a value onto one end of
        the event history and removing all events that are out of
        time.
        
        This is an expensive function, regardless of the type of
        counter.
        """

        # add the new entry to the history list
        self.history.append((syncNumber, value))

        # drop entries beyond the saved region
        tail = syncNumber - self.windowLag - self.windowSize
        self.history = [(s, v) for s, v in self.history if s > tail]

        # reset all counters
        self.clear()

        # fill counters within the active part of the saved region
        head = syncNumber - self.windowLag

        if CUSUM in self.counters:
            self.counters[CUSUM] = []

        if GLR in self.counters:
            self.counters[GLR] = []

        c = self.counters.get(COVARIANCE0, None)

        ols = self.counters.get(OLS0, None)
        if ols is not None:
            ols.clear()

        for s, v in self.history:
            if s <= head:
                if COUNT in self.counters:
                    self.counters[COUNT] += 1

                if SUM1 in self.counters:
                    self.counters[SUM1] += 1.

                if SUMX in self.counters:
                    self.counters[SUMX] += v

                if SUMXX in self.counters:
                    self.counters[SUMXX] += v**2

                if MIN in self.counters:
                    if self.counters[MIN] is None or self.counters[MIN] > v:
                        self.counters[MIN] = v

                if MAX in self.counters:
                    if self.counters[MAX] is None or self.counters[MAX] < v:
                        self.counters[MAX] = v

                if CUSUM in self.counters:
                    cusum = self.counters[CUSUM]
                    if len(cusum) != 1: cusum.append(0.)
                    cusum[0] = max(self.resetValue, cusum[0] + v)

                if GLR in self.counters:
                    self.counters[GLR].append((s, v))

                if c is not None:
                    c[0] += 1.
                    k = 1
                    for i in xrange(len(value)):
                        c[k] += v[i]
                        k += 1
                    for i in xrange(len(value)):
                        for j in xrange(i, len(value)):
                            c[k] += v[i] * v[j]
                            k += 1

                if ols is not None:
                    ols.increment(inboxValue)

class UpdateScheme(object):
    """Factory for creating Updator objects.

    An UpdateScheme is created at the beginning of the job with a
    scheme ('unweighted', 'exponential', 'window', or 'synchronized')
    and parameters.

    Whenever an algorithm wants to make an Updator, it must call the
    updateScheme's updator(counters) function, which has the params
    baked in.
    """

    def __init__(self, scheme, **params):
        self.__dict__.update(params)

        if scheme == "unweighted":
            self.updator = lambda *counters: Updator(counters, params)

        elif scheme == "exponential":
            self.updator = lambda *counters: UpdatorExponential(counters, params)

        elif scheme == "window":
            self.updator = lambda *counters: UpdatorWindow(counters, params)

        elif scheme == "synchronized":
            self.updator = lambda *counters: UpdatorSynchronized(counters, params)

        else:
            raise NotImplementedError("Only 'unweighted', 'exponential', 'window', 'synchronized' schemes have been implemented.")

