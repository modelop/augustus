"""Probability Distribution Functions
"""

__copyright__ = """
Copyright (C) 2005-2007  Open Data ("Open Data" refers to
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
