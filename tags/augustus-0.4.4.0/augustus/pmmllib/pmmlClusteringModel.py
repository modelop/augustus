""""""

__copyright__ = """
Copyright (C) 2006-2010  Open Data ("Open Data" refers to
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

import re, math, numpy, itertools
from pmmlBases import pmmlError, pmmlElementByXSD
from pmmlModelElements import *
from pmmlTree import pmmlPartition
# from itertools import izip

# This file defines PMML elements utilized within k-means models.
# 
# For each element, the same functions are defined that are
# described at the top of the pmmlElements.py file.

pmmlElementByXSD(r"""
  <xs:element name="MissingValueWeights">
    <xs:complexType>
      <xs:sequence>
        <xs:element ref="Extension" minOccurs="0" maxOccurs="unbounded"/>
        <xs:group ref="NUM-ARRAY"/>
      </xs:sequence>
    </xs:complexType>
  </xs:element>
""", globals(), locals())

pmmlElementByXSD(r"""
  <xs:element name="Cluster">
    <xs:complexType>
      <xs:sequence>
        <xs:element ref="Extension" minOccurs="0" maxOccurs="unbounded"/>
        <xs:element ref="KohonenMap" minOccurs="0"/>
        <xs:group ref="NUM-ARRAY" minOccurs="0"/>
        <xs:element ref="Partition" minOccurs="0"/>
        <xs:element ref="Covariances" minOccurs="0"/>
      </xs:sequence>
      <xs:attribute name="name" type="xs:string" use="optional"/>
      <xs:attribute name="size" type="xs:nonNegativeInteger" use="optional"/>
    </xs:complexType>
  </xs:element>
""", globals(), locals())
pmmlCluster.nameNumber = 0
def tmp_pre_init_hook(self, name, attributes, children):
  if attributes.get("name", None) is None:
    attributes["name"] = "Untitled%02d" % pmmlCluster.nameNumber
    pmmlCluster.nameNumber += 1
  return name, attributes, children
pmmlCluster._pre_init_hook = tmp_pre_init_hook; del tmp_pre_init_hook

pmmlElementByXSD(r"""
  <xs:element name="KohonenMap">
    <xs:complexType>
      <xs:sequence>
        <xs:element ref="Extension" minOccurs="0" maxOccurs="unbounded"/>
      </xs:sequence>
      <xs:attribute name="coord1" type="xs:float" use="optional"/>
      <xs:attribute name="coord2" type="xs:float" use="optional"/>
      <xs:attribute name="coord3" type="xs:float" use="optional"/>
      </xs:complexType>
  </xs:element>
""", globals(), locals())

pmmlElementByXSD(r"""
  <xs:element name="Covariances">
    <xs:complexType>
      <xs:sequence>
        <xs:element ref="Extension" minOccurs="0" maxOccurs="unbounded" />
        <xs:element ref="Matrix"/>
      </xs:sequence>
    </xs:complexType>
  </xs:element>
""", globals(), locals())

pmmlElementByXSD(r"""
  <xs:element name="ClusteringField">
    <xs:complexType>
      <xs:sequence>
        <xs:element ref="Extension" minOccurs="0" maxOccurs="unbounded"/>
        <xs:element ref="Comparisons" minOccurs="0"/>
      </xs:sequence>
      <xs:attribute name="field" type="FIELD-NAME" use="required"/>
      <xs:attribute name="isCenterField" default="true">
        <xs:simpleType>
          <xs:restriction base="xs:string">
            <xs:enumeration value="true"/>
            <xs:enumeration value="false"/>
          </xs:restriction>
        </xs:simpleType>
      </xs:attribute>
      <xs:attribute name="fieldWeight" type="REAL-NUMBER" default="1"/>
      <xs:attribute name="similarityScale" type="REAL-NUMBER" use="optional"/>
      <xs:attribute name="compareFunction" type="COMPARE-FUNCTION" use="optional" />
    </xs:complexType>
  </xs:element>
""", globals(), locals())
def tmp_post_init_hook(self, name, attributes, children):
  if self.getAttribute("compareFunction") not in ("absDiff", "gaussSim", "delta", "equal", "table"):
    raise pmmlError, 'pmmlClusteringField:  compareFunction must be one of "absDiff", "gaussSim", "delta", "equal", "table".'

  if self.getAttribute("compareFunction") == "gaussSim":
    try:
      if self.getAttribute("similarityScale") is None: raise ValueError
      float(self.getAttribute("similarityScale"))
    except ValueError:
      raise pmmlError, 'pmmlClusteringField:  similarityScale must exist and must be a number for compareFunction="gaussSim".'

  if self.getAttribute("compareFunction") == "table":
    raise NotImplementedError # temporary cop-out

  if self.getAttribute("fieldWeight") is None:
    self.makeAttribute("fieldWeight", "1.0")
  else:
    try:
      tmp = float(self.getAttribute("fieldWeight"))
    except ValueError:
      raise pmmlError, "pmmlClusteringField:  fieldWeight must be a number."

pmmlClusteringField._post_init_hook = tmp_post_init_hook; del tmp_post_init_hook

pmmlElementByXSD(r"""
  <xs:element name="Comparisons">
    <xs:complexType>
      <xs:sequence>
        <xs:element ref="Extension" minOccurs="0" maxOccurs="unbounded"/>
        <xs:element ref="Matrix"/>
      </xs:sequence>
    </xs:complexType>
  </xs:element>
""", globals(), locals())

pmmlElementByXSD(r"""
  <xs:element name="ComparisonMeasure">
    <xs:complexType>
      <xs:sequence>
        <xs:element ref="Extension" minOccurs="0" maxOccurs="unbounded"/>
        <xs:choice>
          <xs:element ref="euclidean"/>
          <xs:element ref="squaredEuclidean"/>
          <xs:element ref="chebychev"/>
          <xs:element ref="cityBlock"/>
          <xs:element ref="minkowski"/>
          <xs:element ref="simpleMatching"/>
          <xs:element ref="jaccard"/>
          <xs:element ref="tanimoto"/>
          <xs:element ref="binarySimilarity"/>
        </xs:choice>
      </xs:sequence>
      <xs:attribute name="kind" use="required">
        <xs:simpleType>
          <xs:restriction base="xs:string">
            <xs:enumeration value="distance"/>
            <xs:enumeration value="similarity"/>
          </xs:restriction>
        </xs:simpleType>
      </xs:attribute>

      <xs:attribute name="compareFunction" type="COMPARE-FUNCTION" default="absDiff" />
      <xs:attribute name="minimum" type="NUMBER" use="optional"/>
      <xs:attribute name="maximum" type="NUMBER" use="optional"/>
    </xs:complexType>
  </xs:element>
""", globals(), locals())
def tmp_post_init_hook(self, name, attributes, children):
  if len(self.getChildren()) > 1:
    raise pmmlError, "pmmlComparisonMeasure:  only one comparison measure is allowed."
pmmlComparisonMeasure._post_init_hook = tmp_post_init_hook; del tmp_post_init_hook

pmmlElementByXSD(r"""
  <xs:element name="squaredEuclidean">
    <xs:complexType>
      <xs:sequence>
        <xs:element ref="Extension" minOccurs="0" maxOccurs="unbounded" />
      </xs:sequence>
    </xs:complexType>
  </xs:element>
""", globals(), locals())
def metric(self, compareFuncs, isMissing, dataVect, clusterVect, fieldWeights, adjustmentValues):
  adjustnumer = 0.
  adjustdenom = 0.
  sumnonmissing = 0.
  numnonmissing = 0
  for ci, mi, xi, yi, wi, qi in itertools.izip(compareFuncs, isMissing, dataVect, clusterVect, fieldWeights, adjustmentValues):
    adjustnumer += qi
    if not mi:
      adjustdenom += xi*qi
      sumnonmissing += ci(xi, yi)**2 * wi
      numnonmissing += 1
  if numnonmissing == 0 or numnonmissing == len(compareFuncs):
    adjustM = 1.
  else:
    adjustM = adjustnumer / adjustdenom
  return sumnonmissing * adjustM
pmmlsquaredEuclidean.metric = metric; del metric
pmmlsquaredEuclidean.kind = "distance"

pmmlElementByXSD(r"""
  <xs:element name="euclidean">
    <xs:complexType>
      <xs:sequence>
        <xs:element ref="Extension" minOccurs="0" maxOccurs="unbounded" />
      </xs:sequence>
    </xs:complexType>
  </xs:element>
""", globals(), locals())
pmmleuclidean.metric = lambda compareFuncs, isMissing, dataVect, clusterVect, fieldWeights, adjustmentValues: math.sqrt(pmmlsquaredEuclidean.metric(compareFuncs, isMissing, dataVect, clusterVect, fieldWeights, adjustmentValues))
pmmleuclidean.kind = "distance"

pmmlElementByXSD(r"""
  <xs:element name="cityBlock">
    <xs:complexType>
      <xs:sequence>
        <xs:element ref="Extension" minOccurs="0" maxOccurs="unbounded" />
      </xs:sequence>
    </xs:complexType>
  </xs:element>
""", globals(), locals())
def metric(self, compareFuncs, isMissing, dataVect, clusterVect, fieldWeights, adjustmentValues):
  adjustnumer = 0.
  adjustdenom = 0.
  sumnonmissing = 0.
  numnonmissing = 0
  for ci, mi, xi, yi, wi, qi in itertools.izip(compareFuncs, isMissing, dataVect, clusterVect, fieldWeights, adjustmentValues):
    adjustnumer += qi
    if not mi:
      adjustdenom += xi*qi
      sumnonmissing += ci(xi, yi) * wi
      numnonmissing += 1
  if numnonmissing == 0 or numnonmissing == len(compareFuncs):
    adjustM = 1.
  else:
    adjustM = adjustnumer / adjustdenom
  return sumnonmissing * adjustM
pmmlcityBlock.metric = metric; del metric
pmmlcityBlock.kind = "distance"

pmmlElementByXSD(r"""
    <xs:element name="chebychev">
    <xs:complexType>
      <xs:sequence>
        <xs:element ref="Extension" minOccurs="0" maxOccurs="unbounded" />
      </xs:sequence>
    </xs:complexType>
  </xs:element>
""", globals(), locals())
def metric(self, compareFuncs, isMissing, dataVect, clusterVect, fieldWeights, adjustmentValues):
  adjustnumer = 0.
  adjustdenom = 0.
  maxnonmissing = 0.
  numnonmissing = 0
  for ci, mi, xi, yi, wi, qi in itertools.izip(compareFuncs, isMissing, dataVect, clusterVect, fieldWeights, adjustmentValues):
    adjustnumer += qi
    if not mi:
      adjustdenom += xi*qi
      maxnonmissing += ci(xi, yi) * wi
      numnonmissing += 1
  if numnonmissing == 0 or numnonmissing == len(compareFuncs):
    adjustM = 1.
  else:
    adjustM = adjustnumer / adjustdenom
  return maxnonmissing * adjustM
pmmlchebychev.metric = metric; del metric
pmmlchebychev.kind = "distance"

pmmlElementByXSD(r"""
  <xs:element name="minkowski">
    <xs:complexType>
      <xs:sequence>
        <xs:element ref="Extension" minOccurs="0" maxOccurs="unbounded" />
      </xs:sequence>
      <xs:attribute name="p-parameter" type="NUMBER" use="required"/>
    </xs:complexType>
  </xs:element>
""", globals(), locals())
def metric(self, compareFuncs, isMissing, dataVect, clusterVect, fieldWeights, adjustmentValues):
  adjustnumer = 0.
  adjustdenom = 0.
  sumnonmissing = 0.
  numnonmissing = 0
  for ci, mi, xi, yi, wi, qi in itertools.izip(compareFuncs, isMissing, dataVect, clusterVect, fieldWeights, adjustmentValues):
    adjustnumer += qi
    if not mi:
      adjustdenom += xi*qi
      sumnonmissing += ci(xi, yi)**self.p * wi
      numnonmissing += 1
  if numnonmissing == 0 or numnonmissing == len(compareFuncs):
    adjustM = 1.
  else:
    adjustM = adjustnumer / adjustdenom
  return (sumnonmissing * adjustM)**(1./self.p)
pmmlminkowski.metric = metric; del metric
pmmlminkowski.kind = "distance"
def tmp_post_init_hook(self, name, attributes, children):
  try:
    self.p = float(self.getAttribute("p-parameter"))
  except ValueError:
    raise pmmlError, "pmmlminkowski:  p-parameter must be a number."
pmmlminkowski._post_init_hook = tmp_post_init_hook; del tmp_post_init_hook

pmmlElementByXSD(r"""
  <xs:element name="simpleMatching">
    <xs:complexType>
      <xs:sequence>
        <xs:element ref="Extension" minOccurs="0" maxOccurs="unbounded" />
      </xs:sequence>
    </xs:complexType>
  </xs:element>
""", globals(), locals())
def metric(self, dataVect, clusterVect):
  a11 = sum(numpy.logical_and(dataVect, clusterVect))
  a10 = sum(numpy.logical_and(dataVect, numpy.logical_not(clusterVect)))
  a01 = sum(numpy.logical_and(numpy.logical_not(dataVect), clusterVect))
  a00 = sum(numpy.logical_and(numpy.logical_not(dataVect), numpy.logical_not(clusterVect)))
  return float(a11 + a00)/float(a11 + a10 + a01 + a00)
pmmlsimpleMatching.metric = metric; del metric
pmmlsimpleMatching.kind = "similarity"

pmmlElementByXSD(r"""
  <xs:element name="jaccard">
    <xs:complexType>
      <xs:sequence>
        <xs:element ref="Extension" minOccurs="0" maxOccurs="unbounded" />
      </xs:sequence>
    </xs:complexType>
  </xs:element>
""", globals(), locals())
def metric(self, dataVect, clusterVect):
  a11 = sum(numpy.logical_and(dataVect, clusterVect))
  a10 = sum(numpy.logical_and(dataVect, numpy.logical_not(clusterVect)))
  a01 = sum(numpy.logical_and(numpy.logical_not(dataVect), clusterVect))
  a00 = sum(numpy.logical_and(numpy.logical_not(dataVect), numpy.logical_not(clusterVect)))
  return float(a11)/float(a11 + a10 + a01)
pmmljaccard.metric = metric; del metric
pmmljaccard.kind = "similarity"

pmmlElementByXSD(r"""
  <xs:element name="tanimoto">
    <xs:complexType>
      <xs:sequence>
        <xs:element ref="Extension" minOccurs="0" maxOccurs="unbounded" />
      </xs:sequence>
    </xs:complexType>
  </xs:element>
""", globals(), locals())
def metric(self, dataVect, clusterVect):
  a11 = sum(numpy.logical_and(dataVect, clusterVect))
  a10 = sum(numpy.logical_and(dataVect, numpy.logical_not(clusterVect)))
  a01 = sum(numpy.logical_and(numpy.logical_not(dataVect), clusterVect))
  a00 = sum(numpy.logical_and(numpy.logical_not(dataVect), numpy.logical_not(clusterVect)))
  return float(a11 + a00)/float(a11 + 2*(a10+a01) + a00)
pmmltanimoto.metric = metric; del metric
pmmltanimoto.kind = "similarity"

pmmlElementByXSD(r"""
  <xs:element name="binarySimilarity">
    <xs:complexType>
      <xs:sequence>
        <xs:element ref="Extension" minOccurs="0" maxOccurs="unbounded" />
      </xs:sequence>
      <xs:attribute name="c00-parameter" type="NUMBER" use="required"/>
      <xs:attribute name="c01-parameter" type="NUMBER" use="required"/>
      <xs:attribute name="c10-parameter" type="NUMBER" use="required"/>
      <xs:attribute name="c11-parameter" type="NUMBER" use="required"/>
      <xs:attribute name="d00-parameter" type="NUMBER" use="required"/>
      <xs:attribute name="d01-parameter" type="NUMBER" use="required"/>
      <xs:attribute name="d10-parameter" type="NUMBER" use="required"/>
      <xs:attribute name="d11-parameter" type="NUMBER" use="required"/>
    </xs:complexType>
  </xs:element>
""", globals(), locals())
def metric(self, dataVect, clusterVect):
  a11 = sum(numpy.logical_and(dataVect, clusterVect))
  a10 = sum(numpy.logical_and(dataVect, numpy.logical_not(clusterVect)))
  a01 = sum(numpy.logical_and(numpy.logical_not(dataVect), clusterVect))
  a00 = sum(numpy.logical_and(numpy.logical_not(dataVect), numpy.logical_not(clusterVect)))
  return float(self.c11*a11 + self.c10*a10 + self.c01*a01 + self.c00*a00) / float(self.d11*a11 + self.d10*a10 + self.d01*a01 + self.d00*a00)
pmmlbinarySimilarity.metric = metric; del metric
pmmlbinarySimilarity.kind = "similarity"
def tmp_post_init_hook(self, name, attributes, children):
  try:
    self.c00 = float(self.getAttribute("c00-parameter"))
    self.c01 = float(self.getAttribute("c01-parameter"))
    self.c10 = float(self.getAttribute("c10-parameter"))
    self.c11 = float(self.getAttribute("c11-parameter"))
    self.d00 = float(self.getAttribute("d00-parameter"))
    self.d01 = float(self.getAttribute("d01-parameter"))
    self.d10 = float(self.getAttribute("d10-parameter"))
    self.d11 = float(self.getAttribute("d11-parameter"))
  except ValueError:
    raise pmmlError, "pmmlbinarySimilarity:  parameters must be numbers."
pmmlbinarySimilarity._post_init_hook = tmp_post_init_hook; del tmp_post_init_hook

class pmmlClustering:
  """Node element used in the k-means model."""
  _KIND_DISTANCE = 0
  _KIND_SIMILARITY = 1
  _KIND_CODE = {"distance": _KIND_DISTANCE, "similarity": _KIND_SIMILARITY}

  def __init__(self, model):
    self.model = model

  def initialize(self, get, dataInput, localTransDict=None, segment=None):
    self.get = get
    if segment is None:
      self.segment = {}
    else:
      self.segment = segment

    self.clusters = {}
    dimension = None
    for cluster in self.model.getChildrenOfType(pmmlCluster):
      cl = numpy.array(cluster.getChildrenOfType(pmmlArray)[0].array_values, dtype=numpy.float)
      if dimension is not None:
        if dimension != len(cl):
          raise pmmlError, "Cluster:  dimensions of cluster arrays do not match"
      dimension = len(cl)
      self.clusters[cluster.getAttribute("name")] = cl

    missingValueWeights = self.model.getChildrenOfType(pmmlMissingValueWeights)
    if len(missingValueWeights) == 0:
      self.missingWeights = numpy.ones(dimension, dtype=numpy.float)
    else:
      self.missingWeights = numpy.array(missingValueWeights[0].array_values, dtype=numpy.float)

    self.fields = []
    self.compare = []
    self.weights = []
    for field in self.model.getChildrenOfType(pmmlClusteringField):
      self.fields.append(field.getAttribute("field"))
      
      self.compare.append(
        {
        "absDiff": lambda xi, yi: abs(xi - yi),
        "gaussSim": lambda xi, yi: math.exp(-math.log(2.) * (xi - yi)**2 / float(field.getAttribute("similarityScale"))**2),
        "delta": lambda xi, yi: (0 if xi == yi else 1),
        "equal": lambda xi, yi: (1 if xi == yi else 0),
        # "table": NotImplemented
        }[field.getAttribute("compareFunction")])

      self.weights.append(float(field.getAttribute("fieldWeight")))

    if dimension != len(self.fields):
      raise pmmlError, "ClusteringModel:  number of ClusteringFields is not equal to the dimension of the Cluster arrays"

    self.weights = numpy.array(self.weights, dtype=numpy.float)

    self.metric = self.model.getChildrenOfType(pmmlComparisonMeasure)[0].getChildren()[0].metric
    self.kind = self._KIND_CODE[self.model.getChildrenOfType(pmmlComparisonMeasure)[0].getAttribute("kind")]

  def score(self):
    """Return a score from an invocation of 'get'."""

    dataVect = numpy.empty(len(self.fields), dtype=numpy.float)
    isMissing = numpy.empty(len(self.fields), dtype=numpy.bool)
    for i, field in enumerate(self.fields):
      fieldVal = self.get(field)
      if fieldVal is None:
        isMissing[i] = True
        dataVect[i] = 0.
      else:
        isMissing[i] = False
        dataVect[i] = fieldVal

    fieldWeights = self.weights
    adjustmentValues = self.missingWeights

    best = None
    bestCategory = None
    for name, clusterVect in self.clusters.items():
      if self.kind == self._KIND_DISTANCE:
        distance = self.metric(self.compare, isMissing, dataVect, clusterVect, fieldWeights, adjustmentValues)
      elif self.kind == self._KIND_SIMILARITY:
        distance = self.metric(dataVect, clusterVect)

      if best is None or distance < best:
        best = distance
        bestCategory = name

    return [(bestCategory, False, self.segment, None)]
