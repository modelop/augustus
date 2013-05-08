from __future__ import division
__copyright__ = """
Copyright (C) 2008 - 2011  Open Data ("Open Data" refers to
one or more of the following companies: Open Data Partners LLC,
Open Data Research LLC, or Open Data Technologies LLC.)

This is free software; you can redistribute it and/or
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

from optparse import OptionParser, make_option
import random, bisect
import os
from math import *
import time
import sys
import subprocess
import datetime

NSECONDSPERYEAR       = 31536000.0
NSECONDSPERDAY        = 60.*60.*24

FIXEDWIDTH            = True
ELEMENTPADLENGTH      = 19
STRICT                = False


class WeightedRandomPermutationArray(object):
  def __init__(self, items):
    self.items   = []
    self.weights = []
    total        = sum(y for x,y in items.iteritems())
    count        = 0
    for it,weight in items.iteritems():
      count += weight
      self.items.append(it)
      self.weights.append(count)
    self.total = total-1

  def __call__(self):
    rank  = int(random.random() * self.total)
    index = bisect.bisect(self.weights, rank)
    n=self.items[index]
    return self.items[index]


class RecordGenerator(object):
  def __init__(self, out = None, power = -1.0, scale = 0.0):
    self.nseconds    = 0.0   # number of seconds in run.
    self.gen         = random.Random()
    self.power       = power # parameter of power law
    self.inPower     = 1.0 / (1.0-self.power)
    self.scale       = scale # scale number of hits from power law
    self.eventsPerEntity = 0.0
    if out is not None:
      self.out          = file(out,'w')
    else:
      self.out          = sys.stdout
    self.entities       = {}
    self.entitiesUsed   = {}

  def iterateSites(self, startidx, nrecords, hosthash, distfile=None):
    i = 0
    hashstr               = str( hosthash)
    starttime = lasttime  = time.time()
    ntot                  = 0
    calcThreshhold        = float(len(self.entities)) / 100.0
    toRemove              = {}
    self.weightedEntities = WeightedRandomPermutationArray(self.entities)
    self.nsitesGenerated  = 0
    lastcomp              = starttime-NSECONDSPERYEAR
    
    while ( ntot < nrecords ):
      if len(toRemove) >= calcThreshhold:
        for entity, k in toRemove.iteritems():
          del self.entities[entity]
        self.weightedEntities = WeightedRandomPermutationArray(self.entities)
        needsRecalc = 0
        toRemove    = {}
      n = int((self.scale/5.0) * (1-self.power) * self.gen.uniform(0,1)**(-1*self.inPower))
      self.siteid += 1
      self.nsitesGenerated += 1
      siteID = str(self.siteid)
      compsite = 0
      for id in range(0,n):
        cFlag = str(self.gen.randint(0,9))
        # Generate a random entity and a date between now and a year ago:
        entity           = self.weightedEntities()
        time_interval    = random.random() * NSECONDSPERYEAR
        date = datetime.datetime.fromtimestamp( starttime - time_interval )

        self.entitiesUsed.setdefault(entity,0)
        self.entities[entity] -= 1
        if (self.entities[entity] <= 0):
          toRemove[entity] = 1
        
        evntid = ( str(i + startidx).zfill(11) + hashstr.zfill(ELEMENTPADLENGTH + 1) )
        datestr = str(date)
        if len(datestr) == 19:
            datestr += '.000000'
        outrecord = ','.join([evntid, datestr, siteID.zfill(ELEMENTPADLENGTH),
           cFlag,  entity.zfill(ELEMENTPADLENGTH) ])

        ntot += 1
        if STRICT:
            if ntot <= nrecords:
                self.out.write(outrecord+'\n')
            else:
                break
        else:
            self.out.write(outrecord+'\n')

        l = len(outrecord)
        try:
          self.reclength[l] += 1
        except:
          self.reclength = {l:1}

        i += 1

class RecordPrep(RecordGenerator):
  """
  Generate events for compromised sites.
  """
  def __init__(self, power, nrecords, nblocks, nrec_unseeded_blocks, out, ndays, eventsPerEntity, stdEventsPerEntity, scale):

    RecordGenerator.__init__(self, out, power, scale)

    self.siteid           = 0
    self.uniqid           = 0
    self.nseconds         = NSECONDSPERDAY * int(ndays)
    self.eventsPerEntity  = float(eventsPerEntity)
    meanHits              = float(eventsPerEntity) * float(ndays)
    stdHits               = float(stdEventsPerEntity) * float(meanHits)

    count  = 0
    visits = 0

    # number of compromised siites includes 'virtual' sites
    # which can compromise entities external to our known list.
    while (visits< (nrecords + nblocks * nrec_unseeded_blocks)):
      n = int(self.gen.gauss(meanHits,stdHits))
      entity = str(count + 1).zfill(12)
      self.entities[entity] = n
      visits += n
      count += 1
    self.nrec_unseeded_blocks = nrec_unseeded_blocks

  def iterateSites(self, startidx, nrecords, hosthash, distfile=None):

    RecordGenerator.iterateSites(self, startidx, nrecords, hosthash, distfile)

    outdir = os.path.dirname(self.out.name)
    self.out.close()

if __name__=="__main__":
  
  usemsg = "\n \t \t Use nonzero seed_index for non-seed runs. \n \
           For seed runs, the arguments correspond to: \n \
           number of events seed run, \n \
           number of events per non-seed run, and \n \
           total number of non-seed runs, respectively."
  
  usage = "usage: \n \
           Seed Run- \n \
           %prog [options] 0 nrecs nrecsperblock blocks  \n \
           All other runs- \n \
           %prog [options] seed_index \n "+usemsg
  
  misuse = "usage: \n \
           Seed Run- \n \
           malgen.py [options] 0 nrecs nrecsperblock blocks\n \
           All other runs- \n \
           malgen.py [options] seed_index \n "+usemsg
  
  version = "%prog 0.9"
  options = [
    make_option('-P','--power',default=-3.5, 
                help="Power for events per site distribution (default -3.5)"),
    make_option('-D','--ndays',default=365,
                help="Number of days for data sample (default 365)"),
    make_option('-m','--events_per_entity',default=27.0,
                help="Mean number of events per day per entity (default 27)"),
    make_option('-s','--std_events_per_entity',default=.1,
                help="Std Deviation in number of events per day per entity \
as a fraction of the mean number of events per day (default .1)"),
    make_option('-O','--outdir',default='/tmp/',
                help="Directory for output and for seed initialization data (default /tmp/)"),
    make_option('-o','--outfile',default='events-malstone.dat',
                help="Output filename (default events-malstone.dat)"),
    make_option('-S','--sitescale',default=10000,
                help="Scale factor determining typical number of events per site (default 10000)"),
  ]
  parser = OptionParser(usage=usage, version=version, option_list=options)
  (options, arguments) = parser.parse_args()

  hosthash = hash( subprocess.Popen(["hostname"],
                                    stdout=subprocess.PIPE).communicate()[0] )
  try:
    startidx = 0
    STRICT = True
    nrecords = int(arguments[0])
    nrec_unseeded_blocks = 0
    nblocks = 0
  except:
    msg= ' The first arguement must be an integer and should be the desired \n \
number of records to generate.'
    print("%s\n"%msg)
    sys.exit(1)

  eventSetup=RecordPrep(options.power,
                                      nrecords, nblocks,
                                      nrec_unseeded_blocks, 
                                      options.outdir+options.outfile,
                                      options.ndays,
                                      options.events_per_entity,
                                      options.std_events_per_entity,
                                      options.sitescale)
  u=eventSetup.iterateSites(startidx, nrecords, hosthash)

