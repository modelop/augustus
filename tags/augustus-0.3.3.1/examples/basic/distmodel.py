"""ChangeDetectionModel sample implementation.  The following example
  corresponds to the following PMML fragment:

  <BaselineModel modelName="geo-cusum" functionName="baseline" >
   <MiningSchema></MiningSchema>
    <TestDistributions field="cusum-score" testType="threshold" testStatistic="CUSUM" threshold="21.0" resetValue="0.0" >
     <Baseline>
      <GaussianDistribution mean="550.2" variance="48.2" />
     </Baseline>
     <Alternate>
      <GaussianDistribution mean="460.4" variance="39.2" />
     </Alternate>
    </TestDistributions>
  </BaselineModel>


  >>> nullmodel = GaussianDistribution(mean=550.2,variance=48.2)
  >>> altmodel = GaussianDistribution(mean=460.4,variance=39.2)
  >>> cdmodel = ChangeDetectionModel(nullmodel,altmodel,threshold=21.0)
  >>> x = na.asarray([550.0,575.0,600.0,500.0,540.0,545.0,546.0,555.0])

  >>> print cdmodel(x)
  0
  >>> print cdmodel
  +-----+----------------+-----------------+----------------+--------------+--------------+-----+
  | data|   nullmodel    |     altmodel    |      odds      |   log_odds   |    cusum     |score|
  +-----+----------------+-----------------+----------------+--------------+--------------+-----+
  |550.0|0.00827673954471|0.000746690204826| 0.0902155010185|-2.40555401503|           0.0|    0|
  |575.0|0.00725064809844|0.000141819999931|  0.019559630809|-3.93428748935|           0.0|    0|
  |600.0|0.00485355656837|1.79346634281e-05|0.00369515903966|-5.60073168365|           0.0|    0|
  |500.0|0.00481195324661| 0.00610973777985|   1.26970015433|0.238780773641|0.238780773641|    0|
  |540.0|0.00809354298409| 0.00129491745838|  0.159993894012| -1.8326196269|           0.0|    0|
  |545.0|0.00822878411556|0.000991343345606|  0.120472639904|-2.11633260657|           0.0|    0|
  |546.0|0.00824544809883|0.000937934734826|  0.113751820833|-2.17373621396|           0.0|    0|
  |555.0|0.00823587105197| 0.00055333884811| 0.0671864390078| -2.7002838523|           0.0|    0|
  +-----+----------------+-----------------+----------------+--------------+--------------+-----+

  >>> print cdmodel(x-200.0)
  1
  >>> print cdmodel
  +-----+-----------------+-----------------+-------------+-------------+-------------+-----+
  | data|    nullmodel    |     altmodel    |     odds    |   log_odds  |    cusum    |score|
  +-----+-----------------+-----------------+-------------+-------------+-------------+-----+
  |350.0|1.48485905379e-06|0.000192876042254|129.895185513|4.86672785997|4.86672785997|    0|
  |375.0|1.11912071908e-05|0.000948430528613|84.7478303673|4.43968014555|9.30640800552|    0|
  |400.0|6.44516990137e-05| 0.00310521365477|48.1789262703|3.87492171114|13.1813297167|    0|
  |300.0|1.16626974505e-08|2.35450695889e-06|201.883566721|5.30769112885|18.4890208455|    0|
  |340.0|6.13896212052e-07|9.10181141471e-05|148.263032676|4.99898794414|23.4880087896|    1|
  |345.0|9.59901737825e-07|0.000133578291652|139.158297551|4.93561211645|28.4236209061|    1|
  |346.0|1.04831629929e-06|0.000143949319847|137.314777939|4.92227593946|33.3458968456|    1|
  |355.0|2.27232430635e-06|0.000274002822061|120.582621633|4.79233517468|38.1382320202|    1|
  +-----+-----------------+-----------------+-------------+-------------+-------------+-----+


"""

__copyright__ = """
Copyright (C) 2005-2006  Open Data ("Open Data" refers to
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


import sys
import pygsl.rng
import numarray as na
from augustus.unitable import UniTable

class GaussianDistribution(object):
  pdf_func = pygsl.rng.gaussian_pdf
  def __init__(self,mean,variance):
    self.mean = mean
    self.variance = variance
  def __call__(self,data):
    return self.pdf_func(data-self.mean,self.variance)

class ChangeDetectionModel(object):
  def __init__(self,nullmodel,altmodel,threshold,reset_value=0.0):
    self.nullmodel = nullmodel
    self.altmodel = altmodel
    self.threshold = threshold
    self.reset_value = reset_value
    self._state = UniTable()
  def __str__(self): return str(self._state)
  def __call__(self,data):
    state = self._state = UniTable()
    state['data'] = data
    state['nullmodel'] = self.nullmodel(state['data'])
    state['altmodel'] = self.altmodel(state['data'])
    state['odds'] = state['altmodel']/state['nullmodel']
    state['log_odds'] = na.log(state['odds'])
    state['cusum'] = list(gen_cusum(state['log_odds'],self.reset_value))
    state['score'] = state['cusum'] >self.threshold
    return state['score'][-1]
    
def gen_cusum(data,reset_value=0.0):
  # no obvious way to vectorize this
  out = 0.0
  for value in data:
    out = max(reset_value,out+value)
    yield out

########################################################################

if __name__ == "__main__":
  import doctest
  doctest.testmod()

  # batch mode timing tests
  import numarray.random_array as narand
  import time
  nullmodel = GaussianDistribution(mean=550.2,variance=48.2)
  altmodel = GaussianDistribution(mean=460.4,variance=39.2)
  cdmodel = ChangeDetectionModel(nullmodel,altmodel,threshold=21.0)
  for n in range(8):
    data = narand.normal(470.0,88.0,shape=10**n)
    _t0 = time.time()
    out = cdmodel(data)
    _t1 = time.time()
    secs = _t1 - _t0
    print '%s seconds to process %s events (%s events/second) result=%s' \
      % (secs,len(data),len(data)/secs,out)

########################################################################
