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

"""Defines a class that writes PMML models intermittently (in a separate thread)."""

import sys
import os
import logging
import threading, time
try:
    import cPickle as pickle
except ImportError:
    import pickle

from augustus.core.defs import NameSpace
import augustus.core.config as config

class ModelWriterThread(threading.Thread):
    def run(self):
        if self.pickle:
            if self.fileName is None:
                pickle.dump(self.pmmlObject, sys.stdout)
            else:
                pickle.dump(self.pmmlObject, file(self.fileName, "w"))
        else:
            if self.fileName is None:
                self.pmmlObject._xml(self.indent, self.linesep, "", sys.stdout)
            else:
                self.pmmlObject.write(fileName=self.fileName, indent=self.indent, linesep=self.linesep)

        self.timeWriting = time.time() - self.startTime

        metadata = self.modelWriter._metadata
        metadata.data["Models written"] += 1
        metadata.data["Time writing models"] += self.timeWriting

class ModelWriter(object):
    def __init__(
        self,
        baseName,
        serialization=None,
        timeformat="%Y-%m-%d_%H-%M-%S",
        indent="",
        linesep="",
        pickle=False):
        """
        note: serialization, if present, is a dictionary that contains
        {'byEventNumber':True/False, 'rollover':integer_value}
        in which 'rollover' has units of events or seconds.
        """
        self.baseName = baseName
        self.timeformat = timeformat
        self.indent = indent
        self.linesep = linesep
        self.pickle = pickle
        self._logging =  logging.getLogger()
        self._metadata = logging.getLogger('metadata')
        self.thread = None
        if serialization is None:
            self.serialization = None
        else:
            self.serialization = NameSpace(byEventNumber=False)
            if len(serialization) == 0:
                self.serialization['rollover'] = 3600  # hourly
            else:
                if 'byEventNumber' in serialization:
                    self.serialization.byEventNumber = True
                if not self.serialization.byEventNumber:
                    self.serialization['start'] = int(time.time())
                self.serialization['rollover'] = serialization['rollover']

        self.nameCollisions = {}

        # statistics on the writing process
        self._metadata.data["Models written"] = 0
        self._metadata.data["Model write collisions"] = 0
        self._metadata.data["Time writing models"] = 0
        self._metadata.data["Time copying models"] = 0
        self._metadata.data["Time waiting for write thread to unblock"] = 0

    def serialize(self, pmmlObject, event_number):
        if self.serialization is None:
            return
        elif not self.serialization.byEventNumber:
            time_elapsed = int(time.time() - self.serialzation.start)
            if time_elapsed % self.serialization.rollover == 0:
                self.write(pmmlObject)
        else:
            if not isinstance(event_number, (int, long, float)):
                self.write(pmmlObject)
            elif int(event_number) % self.serialization.rollover == 0:
                self.write(pmmlObject)

    def write(self, pmmlObject, identifier=None):
        if self.pickle:
            fileExtension = ".p"
        else:
            fileExtension = ".pmml"

        # write asynchronously without copying if serialization is None
        if self.serialization is None:
            if self.baseName is None:
                fileName = None
            else:
                if identifier is None:
                    fileName = "%s%s" % (self.baseName, fileExtension)
                else:
                    fileName = "%s_%s%s" % (self.baseName, identifier, fileExtension)

            startTime = time.time()
            if self.pickle:
                if fileName is None:
                    pickle.dump(pmmlObject, sys.stdout)
                else:
                    pickle.dump(pmmlObject, file(fileName, "w"))
            else:
                if fileName is None:
                    pmmlObject._xml(self.indent, self.linesep, "", sys.stdout)
                else:
                    pmmlObject.write(fileName=fileName, indent=self.indent, linesep=self.linesep)
            timeWriting = time.time() - startTime

            metadata = self._metadata
            metadata.data["Models written"] += 1
            metadata.data["Time writing models"] += timeWriting

        # if serialization is not None, copy the model and start a new thread
        else:
            startTime = time.time()
            collision = False
            if self.thread is not None:                  # don't allow concurrent writing threads:
                while self.thread.isAlive():             # that can lead to explosive memory usage
                    collision = True

            if collision: self._metadata.data["Model write collisions"] += 1
            self._metadata.data[
                "Time waiting for write thread to unblock"] += \
                time.time() - startTime

            self.thread = ModelWriterThread()
            self.thread.modelWriter = self
            self.thread.identifier = identifier
            self.thread.daemon = False                   # don't quit Python before this is finished writing!

            self.thread.pickle = self.pickle

            startTime = time.time()
            self.thread.pmmlObject = pmmlObject.copy()   # copy the object so that producing can continue during writing
            self.thread.timeCopying = time.time() - startTime
            self._metadata.data["Time copying models"] += \
                self.thread.timeCopying

            theTime = time.strftime(self.timeformat)
            if theTime in self.nameCollisions:
                def baseN(num, b, numerals="abcdefghijklmnopqrstuvwxyz"):
                    return (num == 0 and "a") or (baseN(num // b, b).lstrip("a") + numerals[num % b])
                theExtra = "-%s" % baseN(self.nameCollisions[theTime], 26)
                self.nameCollisions[theTime] += 1
            else:
                theExtra = ""
                self.nameCollisions[theTime] = 0

            if self.baseName is None:
                self.thread.fileName = None
            else:
                if identifier is None:
                    self.thread.fileName = "%s_%s%s%s" % (self.baseName, theTime, theExtra, fileExtension)
                else:            
                    self.thread.fileName = "%s_%s%s_%s%s" % (self.baseName, theTime, theExtra, identifier, fileExtension)

            self.thread.indent = self.indent
            self.thread.linesep = self.linesep

            self.thread.startTime = time.time()
            self.thread.start()

def getModelWriter(configOptions):
    """Call the ModelWriter initialization with given config options.

    Arguments:

        configOptions (XML object, defined in xmlbase):
            The XML element <ModelSetup>...</ModelSetup> which
            contains location and update rate information for model writing.
    """
    if configOptions is None: return None

    filename = configOptions.attrib.get("outputFilename", None)
    if filename is not None:
        if filename.endswith(".pmml"): filename = filename[:-5]
        elif filename.endswith(".p"): filename = filename[:-2]

    #xsd: default is to write the model once, on exit.
    child = configOptions.child(config.Serialization, exception=False)
    if child is None:
        pickle = False
        serialization = None
    else:
        pickle = child.attrib.get("storage", "asPMML") == "asPickle"
        serialization = dict(rollover=None)
        frequency = child.attrib.get("writeFrequency", 1)
        units = child.attrib.get("frequencyUnits", "d")
        if units == "observations":
            serialization.update(byEventNumber=True)
            serialization.update(rollover=int(frequency))
        else:
            serialization.update(byEventNumber=False)
            mult = {'M':60, 'H':3600, 'd':86400}
            serialization.update(rollover=int(frequency) * mult[units])

    return ModelWriter(filename, serialization=serialization, pickle=pickle, indent="    ", linesep=os.linesep)
