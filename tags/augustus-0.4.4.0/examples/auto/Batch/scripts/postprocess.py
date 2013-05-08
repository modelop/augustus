"""
postprocess baseline-batch-0.0.0000
This script is an example of postprocessing output
from scoring with baseline models. It is appropriate
for the case of 'batched' scoring where there is a single
result for each model segment. It performs the following:

1. Record and check versioning information
2. Re-calibrate scores
3. Report scores on a per-segment basis.
"""
def romberg(f, a, b, eps = 1E-8):
    """Approximate the definite integral of f from a to b by Romberg's method.
    eps is the desired accuracy."""
    R = [[0.5 * (b - a) * (f(a) + f(b))]]  # R[0][0]
    n = 1
    while True:
        h = float(b-a)/2**n
        R.append((n+1)*[None])  # Add an empty row.
        R[n][0] = 0.5*R[n-1][0] + h*sum(f(a+(2*k-1)*h) for k in range(1, 2**(n-1)+1)) # for proper limits
        for m in range(1, n+1):
            R[n][m] = R[n][m-1] + (R[n][m-1] - R[n-1][m-1]) / (4**m - 1)
        if abs(R[n][n-1] - R[n][n]) < eps:
            return R[n][n]
        n += 1

def normalize(score):
  psc = 1000*romberg(lambda t: 2/sqrt(pi)*exp(-t*t), 0.,abs(float(score))/sqrt(2.0))
  psc = min(psc, 1000.0)
  if (psc > 995):
    psc = psc/1.0
  elif (psc > 990):
    psc = psc/1.5
  elif (psc > 860):
    psc = psc/1.8
  elif (psc > 340):
    psc = psc/2.0
  else:
    pass
  return psc

# Default tags
scoreTag = 'score'
alertTag = 'alert'
segmentsTag = 'Segments'
observedTag = 'Count'
normScoreFunc = normalize
modelTag = 'BaselineModel'
modelSegmentTags = {'BaselineModel':'TestDistributions'}
modelSegmentTag = modelSegmentTags[modelTag]
testStatisticAttrib = 'testStatistic'

from augustus.kernel.unitable import *
from augustus.external.etree import ElementTree as ET
import sys
import os
import os.path
import datetime
import logging
from math import *

if __name__ == "__main__":
  s = logging.StreamHandler(sys.stdout)
  log = logging.Logger('root')
  log.addHandler(s)
  consumer_config = ET.parse(sys.argv[1])
  config_root = consumer_config.getroot()
  context = config_root.getchildren()
  for config_element in context:
    if config_element.tag == 'inputModel':
      for _m in config_element.getchildren():
        if (_m.tag == 'fromFile'):
          model = _m.attrib['name']
    if config_element.tag == 'output':
      for _m in config_element.getchildren():
        if (_m.tag == 'report'):
          for _r in _m.getchildren():
            if (_r.tag == 'toFile'):
              outputScoresFile = _r.attrib['name']
              consumer_output = ET.parse(outputScoresFile)
  # process pmml for expectations
  if os.path.exists(model):
    pmml = ET.parse(model)    
  elif os.path.exists(os.path.join(os.path.dirname(os.path.abspath(sys.argv[1])),model)):
    model = os.path.join(os.path.dirname(os.path.abspath(sys.argv[1])),model)
    pmml = ET.parse(model)
  else:
    log.critical('CRITICAL: Cannot Find model file')
    raise
    
  pmmlRoot = pmml.getroot()
  try:
    _version = pmmlRoot.findall('.//Application')[0]
  except:
    log.warning('WARNING: Missing Version Information in PMML')
  try:
    version = '-'.join([_version.attrib['name'],_version.attrib['version']])
  except:
    version = 'Unknown'
  for _e in pmmlRoot.getchildren():
    if _e.tag=='BaselineModel':
      tests={}
      for _sg in _e.getchildren():
        _next=[]
        foundsegment={}
        if (_sg.tag=='TestDistributions'):
          score_type=_sg.attrib['testStatistic']
          break

  # process scoring results for observations and scores.

  scores = {}
  segments = {}
  observed = {}
  order = {}

  scoresXML = consumer_output.getroot()
  scoredEvents = scoresXML.getchildren()

  alerts = {}
  for _e in scoredEvents:
    try:
      sc = _e.getchildren()[0].text
    except:
      log.warning('WARNING: Event received with NO SCORE')
      sc = '0.0'

    _dist = []
    for s in _e.getchildren():
      if s.tag == segmentsTag:
        # segment fields are parsed as well as values since it is
        # possible that the same model supports different segmentation schemes.
        seg_elements = s.getchildren()
        eventsegments = dict([(_s.attrib['field'],_s.attrib['value']) for _s in seg_elements])
        segfields = eventsegments.keys()
        segfields.sort()
        segfields = tuple(segfields)
        if segfields not in order.keys():
          order[segfields] = segfields
        segvalues = [eventsegments[_s] for _s in order[segfields]]
        if (segfields not in segments):
          segments[segfields] = []
        if (segvalues) not in segments[segfields]:
          segments[segfields].append(segvalues)
      elif (s.tag == observedTag):
        _dist.append(s.text)
      else:
        pass

    segment = tuple(segvalues)
    sc = str(normScoreFunc(sc))
    # keep track of score and observed values.
    # For this type of post-processing, it is
    # assumed that there is one scoring result
    # per segment.
    try:
      observed[segment]=_dist[0]
    except:
      log.warning('No information on observed values available')
      observed[segment]='Unknown'
    scores[segment]=sc
  for _e in pmmlRoot.getchildren():
    if _e.tag == modelTag:
      tests={}
      for _sg in _e.getchildren():
        _next=[]
        foundsegment={}
        if (_sg.tag == modelSegmentTag):
          score_type=_sg.attrib[testStatisticAttrib]          
          for seg in _sg.getchildren()[1].getchildren():
            foundsegment[seg.attrib['field']]=seg.attrib['value']
          fs=foundsegment.keys()
          fs.sort()
          if (len(fs)>0):
            for _o in order[tuple(fs)]:
              _next.append(foundsegment[_o])
          if (len(_next)>0):
            tests[tuple(_next)]=_sg

  # Fill out xml structure for report.
  output = ET.Element("Report")  
  head = ET.SubElement(output,"head")
  _stamp = ET.SubElement(head,"ProcessingTime")
  _stamp.text = datetime.datetime.now().__str__()
  _model = ET.SubElement(head,"Model")
  _model.text = model
  _config = ET.SubElement(head,"Config")
  _config.text = sys.argv[1]
  _version = ET.SubElement(head,"Version")
  _version.text = version
  events = ET.SubElement(output,"events")
  for segSchema in segments.keys():
    for s in segments[segSchema]:      
      try:
        _score = scores[tuple(s)]
      except:
        #This result is associated with a segment for which no event
        # in the data was noted.
        continue      
      event = ET.SubElement(events,'Event')
      event.attrib['score'] = _score
      _stamp = ET.SubElement(event,"EventTime")
      _stamp.text = datetime.datetime.now().__str__()
      _segment = ET.SubElement(event,"Segment")
      i = 0
      for f in order[segSchema]:
        _segment.attrib[f] = s[i]
        i += 1
      try:
        target = tests[tuple(s)]
      except:
        msg = 'Could not find segment for target'
        log.warning(msg+os.linesep)
        target = ""
        break
      _expected=ET.SubElement(event,"Expected")
      for d in target.getchildren()[0].getchildren():
        if (score_type == 'zTest'):
          _expected.attrib['mean'] = d.attrib['mean']
      _observed = ET.SubElement(event,"Observed")
      try:
        _observed.attrib['Count'] = observed[s]
      except:
        _observed.attrib['Count'] = 'Unknown'
  # Tie it all back together.
  tree = ET.ElementTree(output)
  tree.write(outputScoresFile+".Report")
