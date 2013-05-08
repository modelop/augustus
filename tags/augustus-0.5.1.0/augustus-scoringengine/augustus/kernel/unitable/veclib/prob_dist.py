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

"""Probability Distribution Functions
"""


########################################################################

########################################################################

from base import corelib, EquivUnary, Test, as_num_array
allclose = corelib.allclose

gaussian_pdf = corelib.pygsl.rng.gaussian_pdf
exponential_pdf = corelib.pygsl.rng.exponential_pdf
flat_pdf = corelib.pygsl.rng.flat_pdf
poisson_pdf = corelib.pygsl.rng.poisson_pdf


########################################################################

class GaussianPdf(EquivUnary):
  '''Gaussian probability distribution function

    >>> func = GaussianPdf().gsl
    >>> assert allclose(func([1.2,0.1,0.5],variance=1.0),[0.19418605,0.39695255,0.35206533])


  '''
  name = 'gaussian_pdf'
  ranking = ('gsl',)

  tests = (
    Test([1.2,0.1,0.5],variance=1.0) ** [0.19418605,0.39695255,0.35206533],
  )

  @staticmethod
  def gsl(arg,mean=0.0,variance=0.0,out=None):
    arg = as_num_array(arg)
    if not out:
      out = arg.new()
    if mean == 0.0:
      out[:] = gaussian_pdf(arg-mean,variance)
    else:
      out[:] = gaussian_pdf(arg,variance)
    return out


########################################################################

class ExponentialPdf(EquivUnary):
  '''Exponential probability distribution function

    >>> func = ExponentialPdf().gsl
    >>> assert allclose(func([1.2,0.1,0.5],mu=1.0),[0.30119421,0.90483742,0.60653066])


  '''
  name = 'exponential_pdf'
  ranking = ('gsl',)

  tests = (
    Test([1.2,0.1,0.5],mu=1.0) ** [0.30119421,0.90483742,0.60653066],
  )

  @staticmethod
  def gsl(arg,mu=0.0,out=None):
    arg = as_num_array(arg)
    if not out:
      out = arg.new()
    out[:] = exponential_pdf(arg,mu)
    return out


########################################################################

class PoissonPdf(EquivUnary):
  '''Poisson probability distribution function

    #>>> func = PoissonPdf().gsl
    #>>> func([1,5,55],mu=5.0)


  '''
  name = 'poisson_pdf'
  ranking = ('gsl',)

  @staticmethod
  def gsl(arg,mu=0.0,out=None):
    arg = as_num_array(arg)
    if not out:
      out = arg.new(type="Float")
    out[:] = poisson_pdf(arg,mu)
    return out


########################################################################

class UniformPdf(EquivUnary):
  '''Uniform probability distribution function

    >>> func = UniformPdf().gsl
    >>> assert allclose(func([1.2,0.1,0.5],b=1.0),[0.0,1.0,1.0])


  '''
  name = 'uniform_pdf'
  ranking = ('gsl',)

  tests = (
    Test([1.2,0.1,0.5],b=1.0) ** [0.0,1.0,1.0],
  )

  @staticmethod
  def gsl(arg,a=0.0,b=0.0,out=None):
    arg = as_num_array(arg)
    if not out:
      out = arg.new()
    out[:] = flat_pdf(arg,a,b)
    return out


########################################################################

if __name__ == "__main__":
  from base import tester
  tester.testmod()

########################################################################
