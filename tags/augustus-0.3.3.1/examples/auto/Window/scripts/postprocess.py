"""
postprocess baseline-events-0.0.0000
This script is an example of postprocessing when the segments can
be scored multiple times (i.e. non-batch scoring).
It implements the following actions:

1. Record and check versioning information
2. Re-calibrate scores
3. Report scores only for segments having had a minimum number of alerts
4. Results are reported on a per-segment basis.

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

nAlertThreshold = 45
# Default tags
scoreTag = 'score'
alertTag = 'alert'
segmentsTag = 'Segments'
observedTag = 'distribution'
normScoreFunc = normalize
modelTag = 'BaselineModel'
modelSegmentTags = {'BaselineModel':'TestDistributions'}
modelSegmentTag = modelSegmentTags[modelTag]
testStatisticAttrib = 'testStatistic'

from augustus.kernel.unitable import *
from augustus.external.etree import ElementTree as ET
import sys
import os.path
import datetime
import logging
from math import *

if __name__ == "__main__":
  s = logging.StreamHandler(sys.stdout)
  log = logging.Logger('root')
  log.addHandler(s)
  #Determine files which were used for scoring.
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
    log.critical('CRITICAL: Cannot Find model file\n')
    raise
    

  # Determine model type, name, version
  pmmlRoot = pmml.getroot()
  try:
    _version = pmmlRoot.findall('.//Application')[0]
  except:
    log.warning('WARNING: Missing Version Information in PMML\n')
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
  counts = {}
  segments = {}
  order = {}

  scoresXML = consumer_output.getroot()
  scoredEvents = scoresXML.getchildren()

  alerts = {}
  for _e in scoredEvents:
    try:
      sc = _e.getchildren()[0].text
    except:
      log.warning('WARNING: Event received without a score\n')
      sc = '0.0' 

    alert = _e.find('.//'+alertTag)
    if (alert is not None):
      if (alert.text == 'True'):
         alert = True
      else:
         alert = False
    else:
      log.warning('WARNING: Alert undetermined\n')
      alert = False
    if (alert) or (not alert):
      _dist = []
      for s in _e.getchildren():
        if s.tag == segmentsTag:
          # segment fields are parsed as well as values since it is
          # possible that the same model supports different segmentation schemes.
          seg_elements = s.getchildren()
          eventsegments = dict([(_s.attrib['field'], _s.attrib['value']) for _s in seg_elements])
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
          _dist.append([(b.attrib['index'],b.attrib['val']) for b in s.getchildren()])
        else:
          pass

      # Update score, alert, and observation history for each segment.      
      # For this type of post-processing, there can be
      # many results for each segment, thus, observations
      # and scores are held in lists.
      segment = tuple(segvalues)
      sc = str(normScoreFunc(sc)) # recalibrate score.
      try:
        scores[segment].append(sc)
      except:
        scores[segment] = [sc]
        alerts[segment] = 0
        counts[segment] = []
      try:
        counts[segment].append(_dist[0])
      except:
        log.warning('No information on observed values available\n')
        counts[segment].append([('Unknown','NA')])
      if alert:
        alerts[segment]+=1

  for _e in pmmlRoot.getchildren():
    if _e.tag == modelTag:
      tests={}
      for _sg in _e.getchildren():
        _next = []
        foundsegment = {}
        if (_sg.tag == modelSegmentTag):
          score_type = _sg.attrib[testStatisticAttrib]          
          for seg in _sg.getchildren()[1].getchildren():
            foundsegment[seg.attrib['field']]=seg.attrib['value']
          fs = foundsegment.keys()
          fs.sort()
          if (len(fs) > 0):
            for _o in order[tuple(fs)]:
              _next.append(foundsegment[_o])
          if (len(_next) > 0):
            tests[tuple(_next)] = _sg

  # Create output XML structure

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

  events = ET.SubElement(output,"AlertingSegments")
  for segSchema in segments.keys():
    for segcounts in segments[segSchema]:      
      s = tuple(segcounts)
      try:
        _score=scores[s]
      except:
        #This result is associated with a segment for which no event
        # in the data was noted.
        continue
      if alerts[s]>=nAlertThreshold:
        segment = ET.SubElement(events,"Segment")
        _stamp = ET.SubElement(segment,"PostprocessTime")
        _stamp.text = datetime.datetime.now().__str__()
        i = 0
        for f in order[segSchema]:
          segment.attrib[f]=s[i]
          i+=1  
        # For this configuration, there are multiple
        # scores per segment.      
        for iscore in range(len(scores[s])):
          _score = scores[s][iscore]
          dist = counts[s][iscore]
          event = ET.SubElement(segment,'Event')
          event.attrib['score'] = _score
          _observed=ET.SubElement(event,"Observed")
          if score_type == 'dDist':
            for o in dist:
              count = ET.SubElement(_observed, "Counts")
              count.attrib[o[0]]=o[1]
          elif (score_type == 'zTest'):
            _observed.attrib['N'] = dist
          else:
            pass

        try:
            target=tests[s]
        except:
          msg = 'Could not find segment for target\n'
          msg +=(str(s)+'\n')
          msg += (str(tests) + '\n')
          log.warning('WARNING: '+ msg)
          target=""
          break
        _expected=ET.SubElement(event,"Expected")
        for d in target.getchildren()[0].getchildren():
          if (score_type == 'dDist'):
            pass
          elif (score_type == 'zTest'):
            _expected.attrib['mean']=d.attrib['mean']
          else:
            pass
      else:
        log.info('INFO: Alerts Below threshold for segment '+(' '.join(s))+'\n')
  # Tie it all back together.
  tree = ET.ElementTree(output)
  tree.write(outputScoresFile+".Report")
