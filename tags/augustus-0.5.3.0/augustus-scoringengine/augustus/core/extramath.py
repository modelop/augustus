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

try:
    from sys import float_info
    MAXFLOAT = float_info.max
except ImportError:
    MAXFLOAT = 999999999.9
    try:
        while True:
            MAXFLOAT **= 2
    except OverflowError:
        pass
try:
    from sys import float_info
    MINFLOAT = float_info.min
except ImportError:
    MINFLOAT = 0.00001
    try:
        while True:
            MINFLOAT **= -2
    except OverflowError:
        pass

from math import *

def erf(x):
    """Return the error function of x."""

    # http://stackoverflow.com/questions/457408/is-there-an-easily-available-implementation-of-erf-for-python
    sign = 1
    if x < 0: 
        sign = -1
    x = abs(x)

    a1 =  0.254829592
    a2 = -0.284496736
    a3 =  1.421413741
    a4 = -1.453152027
    a5 =  1.061405429
    p  =  0.3275911

    # http://www.amazon.com/dp/0486612724/?tag=stackoverfl08-20 formula 7.1.26
    t = 1.0/(1.0 + p*x)
    y = 1.0 - (((((a5*t + a4)*t) + a3)*t + a2)*t + a1)*t*exp(-x*x)
    return sign*y # erf(-x) = -erf(x)

def erfinv(y):
    """Return the inverse error function of y."""

    # http://stackoverflow.com/questions/5971830/need-code-for-inverse-error-function
    # note: if we need a different one, try http://home.online.no/~pjacklam/notes/invnorm/
    a = [0.886226899, -1.645349621,  0.914624893, -0.140543331]
    b = [-2.118377725,  1.442710462, -0.329097515,  0.012229801]
    c = [-1.970840454, -1.624906493,  3.429567803,  1.641345311]
    d = [3.543889200,  1.637067800]
    y0 = 0.7

    if not (-1. <= y <= 1.):
        raise ValueError("erfinv argument must be between -1. and 1. (inclusive)")

    if y == -1.:
        return -MAXFLOAT

    if y == 1.:
        return MAXFLOAT

    if y < -y0:
        z = sqrt(-log((1. + y)/2.))
        x = -(((c[3]*z + c[2])*z + c[1])*z + c[0])/((d[1]*z + d[0])*z + 1.0)
    else:
        if y < y0:
            z = y**2
            x = y*(((a[3]*z + a[2])*z + a[1])*z + a[0])/((((b[3]*z + b[3])*z + b[1])*z + b[0])*z + 1.)
        else:
            z = sqrt(-log((1. - y)/2.))
            x = (((c[3]*z + c[2])*z + c[1])*z + c[0])/((d[1]*z + d[0])*z + 1.)

        x = x - (erf(x) - y) / (2./sqrt(pi) * exp(-x**2))
        x = x - (erf(x) - y) / (2./sqrt(pi) * exp(-x**2))
    return x

def chiSquare_cdf(chi2, ndf):
    # http://root.cern.ch ROOT::TMath

    if ndf == 0: return 0.
    if chi2 <= 0.: return 0.

    if ndf == 1:
        return erf(sqrt(chi2)/sqrt(2.))

    q = sqrt(2.*chi2) - sqrt(2.*ndf - 1.)
    if ndf > 30 and q > 10.:
        # Gaussian approximation
        return erf(q/sqrt(2.))

    return 1. - gammq(0.5*ndf, 0.5*chi2)

# gamma functions from Tom Loredo (http://www.johnkerl.org/python/sp_funcs_m.py.txt)
def gammln(xx):
    """Logarithm of the gamma function."""
    gammln_cof = [76.18009173, -86.50532033, 24.01409822, -1.231739516e0, 0.120858003e-2, -0.536382e-5]
    gammln_stp = 2.50662827465

    x = xx - 1.
    tmp = x + 5.5
    tmp = (x + 0.5)*log(tmp) - tmp
    ser = 1.0
    for j in xrange(6):
        x = x + 1.0
        ser = ser + gammln_cof[j]/x
    return tmp + log(gammln_stp*ser)

def gamma(x):
    """Gamma function."""
    return exp(gammln(x))

def beta(a, b):
    """Beta function."""
    return exp(gammln(a) + gammln(b) - gammln(a + b))

def gser(a, x, itmax=700, eps=3.e-7):
    """Series approx'n to the incomplete gamma function."""
    gln = gammln(a)
    if x < 0.:
        raise ValueError("gser x argument must be greater than zero (inclusive)")
    if x == 0.:
        return 0.
    ap = a
    summ = 1.0 / a
    delta = summ
    n = 1
    while n <= itmax:
        ap = ap + 1.0
        delta = delta * x / ap
        summ = summ + delta
        if abs(delta) < abs(summ)*eps:
            return summ * exp(-x + a*log(x) - gln), gln
        n = n + 1
    raise RuntimeError("gser reached the maximum number of iterations")

def gcf(a, x, itmax=200, eps=3.e-7):
    """Continued fraction approx'n of the incomplete gamma function."""
    gln = gammln(a)
    gold = 0.0
    a0 = 1.0
    a1 = x
    b0 = 0.0
    b1 = 1.0
    fac = 1.0
    n = 1
    while n <= itmax:
        an = n
        ana = an - a
        a0 = (a1 + a0*ana)*fac
        b0 = (b1 + b0*ana)*fac
        anf = an*fac
        a1 = x*a0 + anf*a1
        b1 = x*b0 + anf*b1
        if a1 != 0.:
            fac = 1. / a1
            g = b1*fac
            if abs((g-gold)/g) < eps:
                return g*exp(-x+a*log(x)-gln), gln
            gold = g
        n = n + 1
    raise RuntimeError("gcf reached the maximum number of iterations")

def gammp(a, x):
    """Incomplete gamma function."""
    if x < 0.:
        raise ValueError("gammp x must be greater than or equal to zero")
    if a <= 0.:
        raise ValueError("gammp a must be greater than zero")

    if x < a + 1.:
        return gser(a,x)[0]
    else:
        return 1. - gcf(a,x)[0]

def gammq(a, x):
    """Incomplete gamma function."""
    if x < 0.:
        raise ValueError("gammq x must be greater than or equal to zero")
    if a <= 0.:
        raise ValueError("gammq a must be greater than zero")

    if x < a + 1.:
        return 1. - gser(a,x)[0]
    else:
        return gcf(a,x)[0]
