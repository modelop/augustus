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

"""This module defines the NumpyInterface class and a few vectorized functions that are unavailable in Numpy."""

import numpy

class NumpyInterface(object):
    """NumpyInterface wraps all NumPy calls so that we can intercept
    them for various purposes.

    NP is intended to be the only instance of this class.
    """

    def __init__(self):
        """Create a NumpyInterface."""

        self._numberOfArrays = 0
        self._numberOfBytes = 0
        self._arraysObserved = set()

    def __call__(self, func, *args, **kwds):
        """Call a Numpy function.

        @type func: string
        @param func: Name of the Numpy function.
        @param *args, **kwds: Arguments passed to the Numpy function.
        @rtype: any
        @return: Data returned by Numpy.
        """

        if isinstance(func, basestring):
            result = getattr(numpy, func)(*args, **kwds)
        else:
            result = func

        if isinstance(result, numpy.ndarray):
            if id(result) not in self._arraysObserved:
                self._numberOfArrays += 1
                self._numberOfBytes += result.nbytes
                self._arraysObserved.add(id(result))

        elif isinstance(result, tuple):
            for x in result:
                if isinstance(x, numpy.ndarray):
                    if id(x) not in self._arraysObserved:
                        self._numberOfArrays += 1
                        self._numberOfBytes += x.nbytes
                        self._arraysObserved.add(id(x))
                    
        return result

    def __getattr__(self, name, noneIfMissing=False):
        """Return an object from the C{numpy} module's namespace.

        @type name: string
        @param name: Name of the Numpy object.
        @rtype: any
        @return: Object from Numpy.
        """

        if noneIfMissing and not hasattr(numpy, name):
            return None
        else:
            return getattr(numpy, name)

NP = NumpyInterface()

#######################################################################

def erf(array):
    """Vectorized calculation of the error function (erf) across a Numpy array.

    Numpy does not have a native implementation of erf.
    U{Scipy does <http://docs.scipy.org/doc/scipy/reference/generated/scipy.special.erf.html>},
    and there is a non-vectorized erf in the Python Standard Library,
    but the former introduces a dependency and the latter should not
    be used on large arrays.

    If Scipy is available on your system, C{NumpyInterface.erf} will
    be replaced by Scipy's implementation.

    @type array: 1d Numpy array
    @param array: Array of x to compute erf(x).
    @rtype: 1d Numpy array
    @return: Array of erf(x).
    @see: From a post on U{StackOverflow <http://stackoverflow.com/questions/457408/is-there-an-easily-available-implementation-of-erf-for-python>}, which references formula 7.1.26 of U{this book<http://www.amazon.com/dp/0486612724/?tag=stackoverfl08-20>}.
    """

    sign = NP("where", array < 0, -1.0, 1.0)
    array = NP("absolute", array)

    a1 =  0.254829592
    a2 = -0.284496736
    a3 =  1.421413741
    a4 = -1.453152027
    a5 =  1.061405429
    p  =  0.3275911

    t = NP(1.0 / NP(1.0 + NP(array * p)))
    y = NP(1.0 - NP(NP(NP(NP(NP(NP(NP(NP(a5*t) + a4)*t) + a3)*t + a2)*t + a1)*t) * NP("exp", NP("negative", NP("square", array)))))
    return NP(sign * y) # erf(-x) = -erf(x)

try:
    import scipy.special
except ImportError:
    pass
else:
    erf = lambda x: NP(scipy.special.erf(x))

def gammaln(array):
    """Vectorized calculation of ln(abs(gamma(array))) across a Numpy array.

    Numpy does not have a native implementation of gammaln.
    U{Scipy does <http://docs.scipy.org/doc/scipy/reference/generated/scipy.special.gammaln.html>},
    but that would introduce a dependency.

    If Scipy is available on your system, C{NumpyInterface.gammaln}
    will be replaced by Scipy's version.

    @type array: 1d Numpy array
    @param array: Array of x to compute gammaln(x).
    @rtype: 1d Numpy array
    @return: Array of gammaln(x).
    @see: Source: U{Tom Loredo <http://www.johnkerl.org/python/sp_funcs_m.py.txt>}.
    """

    gammaln_cof = [76.18009173, -86.50532033, 24.01409822, -1.231739516e0, 0.120858003e-2, -0.536382e-5]
    gammaln_stp = 2.50662827465
    x = NP(array - 1.0)
    tmp = NP(x + 5.5)
    tmp = NP(NP(NP(x + 0.5)*NP("log", tmp)) - tmp)
    ser = NP("ones", len(array), dtype=NP.dtype(float))
    for cof in gammaln_cof:
        x += 1.0
        ser += cof/x
    return NP(tmp + NP("log", NP(gammaln_stp*ser)))

try:
    import scipy.special
except ImportError:
    pass
else:
    gammaln = lambda x: NP(scipy.special.gammaln(x))

def _gser(array, x, iterations=100):
    """Used by gammainc."""

    gln = gammaln(array)
    if x < 0.:
        raise ValueError("_gser x argument must be greater than zero (inclusive)")
    if x == 0.0:
        return NP("zeros", len(array), dtype=NP.dtype(float))
    ap = NP("array", array)
    summ = NP("reciprocal", array)
    delta = NP("array", summ)
    for n in xrange(iterations):
        ap += 1.0
        delta *= x
        delta /= ap
        summ += delta
    return NP(summ * NP("exp", NP(NP(-x + NP(array*NP("log", x))) - gln)))

def _gcf(array, x, iterations=100):
    """Used by gammainc."""

    gln = gammaln(array)
    gold = 0.0
    a0 = NP("ones", len(array), dtype=NP.dtype(float))
    a1 = x
    b0 = NP("zeros", len(array), dtype=NP.dtype(float))
    b1 = 1.0
    fac = NP("ones", len(array), dtype=NP.dtype(float))
    g = NP("zeros", len(array), dtype=NP.dtype(float))
    for n in xrange(iterations):
        an = n + 1.0
        ana = NP(an - array)
        a0 = NP(NP(a1 + NP(a0*ana))*fac)
        b0 = NP(NP(b1 + NP(b0*ana))*fac)
        anf = NP(an*fac)
        a1 = NP(x*a0) + NP(anf*a1)
        b1 = NP(x*b0) + NP(anf*b1)
        selection = NP(a1 != 0.0)
        fac[selection] = NP("reciprocal", a1[selection])
        g[selection] = NP(b1[selection] * fac[selection])

    return NP(g * NP("exp", NP(NP(-x + NP(array * NP("log", x))) - gln)))

def gammainc(a, array):
    """Vectorized calculation of the incomplete gamma integral 1 / gamma(a) * integral(exp(-t) * t**(a-1), t=0..x) across a Numpy array.

    Numpy does not have a native implementation of gammainc.
    U{Scipy does <http://docs.scipy.org/doc/scipy/reference/generated/scipy.special.gammainc.html>},
    but that would introduce a dependency.

    If Scipy is available on your system, C{NumpyInterface.gammainc}
    will be replaced by Scipy's version.

    @type array: 1d Numpy array
    @param array: Array of x to compute gammainc(x).
    @rtype: 1d Numpy array
    @return: Array of gammainc(x).
    @see: Source: U{Tom Loredo <http://www.johnkerl.org/python/sp_funcs_m.py.txt>}.
    """

    output = NP("empty", len(array), dtype=NP.dtype(float))

    selection = NP(a < NP(array + 1.0))
    output[selection] = NP(1.0 - _gser(array[selection], a))

    NP("logical_not", selection, selection)
    output[selection] = _gcf(array[selection], a)

    return output

try:
    import scipy.special
except ImportError:
    pass
else:
    gammainc = lambda x: NP(scipy.special.gammainc(x))
