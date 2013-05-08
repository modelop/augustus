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

"""This module defines the MapReduce class."""

import math
import threading
import logging
import types
import subprocess
import os
import re
import random
import string
import copy
import struct
import time
import tempfile
try:
    import cPickle as pickle
except ImportError:
    import pickle
try:
    from cStringIO import StringIO
except ImportError:
    try:
        from StringIO import StringIO
    except ImportError:
        from io import BytesIO as StringIO

import numpy

from augustus.core.PerformanceTable import PerformanceTable
from augustus.core.FakePerformanceTable import FakePerformanceTable
from augustus.mapreduce.MapReduceApplication import MapReduceApplication
from augustus.mapreduce.MapReduceTemplate import BUILTIN_GLOBALS, PICKLE_PROTOCOL, serializeClass, unserializeClass

class MapReduce(object):
    """MapReduce implements two kinds of map-reduce jobs, one in pure
    Python, for testing, the other in Hadoop, for large-scale
    deployment.

    @type HADOOP_EXECUTABLE: string or None
    @param HADOOP_EXECUTABLE: To use Hadoop, you must first set this class attribute to the location of the C{hadoop} command on your system.  For example, Cloudera's is C{/usr/bin/hadoop}.
    @type HADOOP_STREAMING_JAR: string or None
    @param HADOOP_STREAMING_JAR: To use Hadoop, you must first set this class attribute to the location of the Hadoop streaming jar on your system.  For example, Cloudera's is C{/usr/lib/hadoop-version-mapreduce/contrib/streaming/hadoop-streaming-version.jar}.
    """

    HADOOP_EXECUTABLE = None
    HADOOP_STREAMING_JAR = None
    mapReduceApplicationBaseClass = serializeClass(MapReduceApplication)
    loggerName = "Augustus.MapReduce"

    def __init__(self, *mapReduceApplications):
        """Initialize a map-reduce workflow that may be run in pure
        Python or submitted to Hadoop.

        @type *mapReduceApplications: list of MapReduceApplications
        @param *mapReduceApplications: MapReduceApplication classes that define a workflow.  If there is only one class, that class will be used for all iterations.  If there are N, the first N-1 will be used for the first N-1 iterations, and the last one will be used for all subsequent iterations.
        """

        if len(mapReduceApplications) == 0:
            raise ValueError("MapReduce must be given at least one MapReduceApplication subclass")

        self.mapRedApps = mapReduceApplications
        self.mapRedAppsSerialized = map(serializeClass, mapReduceApplications)
        self.metadata = mapReduceApplications[0].metadata
        self.chain = [x.chain for x in mapReduceApplications]

        self.done = False
        self._performanceTables = []
        self.logger = logging.getLogger(self.loggerName)

    @property
    def performanceTable(self):
        if len(self._performanceTables) == 0:
            return PerformanceTable()
        else:
            return PerformanceTable.combine(self._performanceTables)

    ### Pure-Python map-reduce, for testing

    class Controller(object):
        def __init__(self, mapReduceApplication):
            self.mapReduceApplication = mapReduceApplication

        def mapper(self, data):
            self.mapReduceApplication.beginMapperTask()
            for datum in data:
                self.mapReduceApplication.mapper(datum)
            self.mapReduceApplication.endMapperTask()

        def reducer(self, data):
            self.mapReduceApplication.beginReducerTask()

            lastkey = None
            for key, value in data:
                if key != lastkey:
                    if lastkey is not None:
                        self.mapReduceApplication.endReducerKey(lastkey)
                    self.mapReduceApplication.beginReducerKey(key)

                self.mapReduceApplication.reducer(key, value)
                lastkey = key

            if lastkey is not None:
                self.mapReduceApplication.endReducerKey(lastkey)

    def run(self, inputData, iterationLimit=None, sort=False, parallel=False, numberOfMappers=1, numberOfReducers=1, frozenClass=True, performanceTable=None):
        """Run the whole pure-Python map-reduce workflow, which may involve multiple iterations of map-reduce.

        @type inputData: list of Python objects
        @param inputData: The objects to use as a data stream.
        @type iterationLimit: int
        @param iterationLimit: The highest iteration to allow, with 1 being the first iteration.
        @type sort: bool
        @param sort: If True, perform a sorting step between the mapper and the reducer.
        @type parallel: bool
        @param parallel: If True, run the independent mappers and independent reducers as distinct threads.
        @type numberOfMappers: int
        @param numberOfMappers: Requested number of mappers.  Input data will be divided evenly among them.
        @type numberOfReducers: int
        @param numberOfReducers: Requested number of reducers.
        @type frozenClass: bool
        @param frozenClass: If True, practice serializing and unserializing the class to ensure the independence of the mappers and the reducers.  If False, skip this performance-limiting step.
        @type performanceTable: PerformanceTable or None
        @param performanceTable: A PerformanceTable for measuring the efficiency of the calculation.
        @rtype: 3-tuple of list, dict, int
        @return: List of output records, dictionary of output key-value pairs, and the final iteration number.
        """

        self.logger.info("Start run with %d input records and %d cache keys.", len(inputData), len(self.metadata))

        overheadPerformanceTable = PerformanceTable()

        if performanceTable is None:
            self._performanceTables = [overheadPerformanceTable]
        else:
            self._performanceTables = [performanceTable, overheadPerformanceTable]

        startTime = time.time()
        overheadPerformanceTable.begin("MapReduce.run")
        
        iteration = 0
        while True:
            if iteration < len(self.chain):
                chain = self.chain[iteration]
            else:
                chain = self.chain[-1]

            overheadPerformanceTable.pause("MapReduce.run")
            outputRecords, outputKeyValues = self.iterate(inputData, iteration, sort, parallel, numberOfMappers, numberOfReducers, frozenClass)
            overheadPerformanceTable.unpause("MapReduce.run")

            if self.done:
                break

            if chain:
                inputData = outputRecords
                self.logger.info("Chaining output to next iteration (%d intermediate records).", len(outputRecords))
            else:
                self.logger.info("Ignoring iteration output (%d ignored records).", len(outputRecords))

            iteration += 1
            if iterationLimit is not None and iteration >= iterationLimit:
                self.logger.info("Reached iteration limit of %d.", iterationLimit)
                self.done = True
                break

        overheadPerformanceTable.end("MapReduce.run")
        endTime = time.time()

        if self.logger.isEnabledFor(logging.INFO):
            stream = StringIO()
            stream.write("%sPerformanceTable (concurrent times are added together; results are in CPU-seconds):%s%s" % (os.linesep, os.linesep, os.linesep))

            self.performanceTable.look(stream=stream)
            stream.write("%sTotal wall time, including any concurrency: %g%s" % (os.linesep, endTime - startTime, os.linesep))
            self.logger.info(stream.getvalue())

        self.logger.info("Finished run with %d output records.", len(outputRecords))
        return outputRecords, outputKeyValues, iteration

    def _importModule(self, name):
        mod = __import__(name)
        components = name.split(".")
        for comp in components[1:]:
            mod = getattr(mod, comp)
        return mod

    def _buildNamespace(self, imports):
        namespace = BUILTIN_GLOBALS.copy()
        for name, value in imports.items():
            if value is None:                         # import name
                namespace[name] = self._importModule(name)

            elif isinstance(value, basestring):       # import name as value
                namespace[value] = self._importModule(name)

            else:
                for n, v in self._importModule(name).__dict__.items():
                    if n in value:
                        try:
                            namespace[value[n]] = v   # from name import value1 as v1, value2 as v2, ...
                        except TypeError:
                            namespace[n] = v          # from name import value1, value2, ...
        return namespace

    def iterate(self, inputData, iteration=0, sort=False, parallel=False, numberOfMappers=1, numberOfReducers=1, frozenClass=True):
        """Run a pure-Python map-reduce iteration.

        @type inputData: list of Python objects
        @param inputData: The objects to use as a data stream.
        @type iteration: int
        @param iteration: The iteration number.
        @type sort: bool
        @param sort: If True, perform a sorting step between the mapper and the reducer.
        @type parallel: bool
        @param parallel: If True, run the independent mappers and independent reducers as distinct threads.
        @type numberOfMappers: int
        @param numberOfMappers: Requested number of mappers.  Input data will be divided evenly among them.
        @type numberOfReducers: int
        @param numberOfReducers: Requested number of reducers.
        @type frozenClass: bool
        @param frozenClass: If True, practice serializing and unserializing the class to ensure the independence of the mappers and the reducers.  If False, skip this performance-limiting step.
        @rtype: 2-tuple of list, dict
        @return: List of output records and dictionary of output key-value pairs.
        """

        overheadPerformanceTable = PerformanceTable()
        self._performanceTables.append(overheadPerformanceTable)
        overheadPerformanceTable.begin("MapReduce.iterate")

        if iteration < len(self.mapRedApps):
            mapRedApp = self.mapRedApps[iteration]
            mapRedAppSerialized = self.mapRedAppsSerialized[iteration]
        else:
            mapRedApp = self.mapRedApps[-1]
            mapRedAppSerialized = self.mapRedAppsSerialized[-1]

        gatherOutput = mapRedApp.gatherOutput
        imports = mapRedApp.imports
        if imports is None:
            imports = {}
        namespace = self._buildNamespace(imports)

        self.logger.info("Start iteration %d with %d input records and %d metadata keys.", iteration, len(inputData), len(self.metadata))
        
        self.done = False
        overheadPerformanceTable.begin("copy metadata")
        startMetadata = copy.deepcopy(self.metadata)
        overheadPerformanceTable.end("copy metadata")

        intermediateData = {}
        dataLock = threading.Lock()
        def emit(appself, key, record):
            with dataLock:
                newTuple = copy.deepcopy((key, record))

                if key in intermediateData:
                    self.logger.debug("    key \"%s\": %r", key, record)
                    intermediateData[key].append(newTuple)

                else:
                    self.logger.debug("New key \"%s\": %r", key, record)
                    intermediateData[key] = [newTuple]

        if parallel:
            mapperThreads = []

        recordsPerMapper = int(math.ceil(len(inputData) / float(numberOfMappers)))
        for number in xrange(numberOfMappers):
            subData = inputData[(number * recordsPerMapper):((number + 1) * recordsPerMapper)]

            performanceTable = PerformanceTable()
            self._performanceTables.append(performanceTable)

            overrideAttributes = {"metadata": startMetadata, "iteration": iteration, "emit": emit, "performanceTable": performanceTable, "logger": logging.getLogger(mapRedApp.loggerName)}

            if frozenClass:
                overheadPerformanceTable.begin("unfreeze mapper")
                appClass = unserializeClass(mapRedAppSerialized, MapReduceApplication, overrideAttributes, namespace)
                overheadPerformanceTable.end("unfreeze mapper")
                controller = self.Controller(appClass())
            else:
                appInstance = mapRedApp()
                overrideAttributes["emit"] = types.MethodType(overrideAttributes["emit"], appInstance)
                appInstance.__dict__.update(overrideAttributes)
                controller = self.Controller(appInstance)

            if parallel:
                self.logger.info("Starting mapper %d in parallel with %d input records.", number, len(subData))
                mapperThreads.append(threading.Thread(target=controller.mapper, name=("Mapper_%03d" % number), args=(subData,)))
            else:
                self.logger.info("Starting mapper %d in series with %d input records.", number, len(subData))
                overheadPerformanceTable.pause("MapReduce.iterate")
                controller.mapper(subData)
                overheadPerformanceTable.unpause("MapReduce.iterate")

        if parallel:
            overheadPerformanceTable.pause("MapReduce.iterate")
            for thread in mapperThreads:
                thread.start()
            for thread in mapperThreads:
                thread.join()
            overheadPerformanceTable.unpause("MapReduce.iterate")

        self.logger.info("All mappers finished.")

        if sort:
            self.logger.info("Sorting %d intermediate values.", sum(len(x) for x in intermediateData.values()))
            overheadPerformanceTable.begin("sort intermediate data")
            for value in intermediateData.values():
                value.sort()
            overheadPerformanceTable.end("sort intermediate data")
        else:
            self.logger.info("Leaving %d intermediate values in the order in which they were generated.", sum(len(x) for x in intermediateData.values()))

        overheadPerformanceTable.begin("load balance")

        lengths = [(key, len(intermediateData[key])) for key in intermediateData]
        lengths.sort(lambda a, b: cmp(b[1], a[1]))

        assignments = [[] for x in xrange(numberOfReducers)]
        workload = [0] * numberOfReducers
        for key, length in lengths:
            index = min((w, i) for i, w in enumerate(workload))[1]   # this is argmin(workload)
            assignments[index].append(key)
            workload[index] += length
        
        if self.logger.isEnabledFor(logging.INFO):
            for i, (a, w) in enumerate(zip(assignments, workload)):
                if len(a) > 10:
                    self.logger.info("Assigning %d keys (%d total records) to reducer %d.", len(a), w, i)
                else:
                    self.logger.info("Assigning keys %s (%d total records) to reducer %d.", ", ".join("\"%s\"" % k for k in a), w, i)

        overheadPerformanceTable.end("load balance")

        outputRecords = []
        outputKeyValues = {}
        dataLock = threading.Lock()
        def emit(appself, key, record):
            if key is None:
                self.logger.debug("OutputRecord: %r", record)
                outputRecords.append(record)
            else:
                with dataLock:
                    if key in outputKeyValues:
                        raise RuntimeError("Two reducers are trying to write to the same metadata key: \"%s\"" % key)
                    else:
                        self.logger.debug("OutputKeyValue \"%s\": %r", key, record)
                        outputKeyValues[key] = record

        if parallel:
            reducerThreads = []

        for number in xrange(numberOfReducers):
            subData = []
            for key in assignments[number]:
                subData.extend(intermediateData[key])

            performanceTable = PerformanceTable()
            self._performanceTables.append(performanceTable)

            overrideAttributes = {"metadata": startMetadata, "iteration": iteration, "emit": emit, "performanceTable": performanceTable, "logger": logging.getLogger(mapRedApp.loggerName)}
            if frozenClass:
                overheadPerformanceTable.begin("unfreeze reducer")
                appClass = unserializeClass(mapRedAppSerialized, MapReduceApplication, overrideAttributes, namespace)
                overheadPerformanceTable.end("unfreeze reducer")
                controller = self.Controller(appClass())
            else:
                appInstance = mapRedApp()
                overrideAttributes["emit"] = types.MethodType(overrideAttributes["emit"], appInstance)
                appInstance.__dict__.update(overrideAttributes)
                controller = self.Controller(appInstance)            
            
            if parallel:
                self.logger.info("Starting reducer %d in parallel with %d input records.", number, len(subData))
                reducerThreads.append(threading.Thread(target=controller.reducer, name=("Reducer_%03d" % number), args=(subData,)))
            else:
                self.logger.info("Starting reducer %d in series with %d input records.", number, len(subData))
                overheadPerformanceTable.pause("MapReduce.iterate")
                controller.reducer(subData)
                overheadPerformanceTable.unpause("MapReduce.iterate")

        if parallel:
            overheadPerformanceTable.pause("MapReduce.iterate")
            for thread in reducerThreads:
                thread.start()
            for thread in reducerThreads:
                thread.join()
            overheadPerformanceTable.unpause("MapReduce.iterate")

        self.logger.info("All reducers finished.")
        self.logger.info("Finished iteration %s with %d output records and %d metadata keys.", iteration, len(outputRecords), len(outputKeyValues))

        if gatherOutput:
            performanceTable = PerformanceTable()
            self._performanceTables.append(performanceTable)

            overrideAttributes = {"metadata": startMetadata, "iteration": iteration, "emit": None, "performanceTable": performanceTable, "logger": logging.getLogger(mapRedApp.loggerName)}

            if frozenClass:
                overheadPerformanceTable.begin("unfreeze endIteration")
                appClass = unserializeClass(mapRedAppSerialized, MapReduceApplication, overrideAttributes, namespace)
                overheadPerformanceTable.end("unfreeze endIteration")
                appInstance = appClass()
            else:
                appInstance = mapRedApp()
                appInstance.__dict__.update(overrideAttributes)

            overheadPerformanceTable.pause("MapReduce.iterate")
            if appInstance.endIteration(outputRecords, outputKeyValues):
                self.done = True
            overheadPerformanceTable.unpause("MapReduce.iterate")

            self.metadata = appInstance.metadata

            overheadPerformanceTable.end("MapReduce.iterate")
            return outputRecords, outputKeyValues

        else:
            self.metadata = startMetadata
            overheadPerformanceTable.end("MapReduce.iterate")
            return [], {}

    ### Hadoop, for deployment

    @classmethod
    def _hadoopCheck(cls):
        if cls.HADOOP_EXECUTABLE is None or cls.HADOOP_STREAMING_JAR is None:
            raise IOError("MapReduce.HADOOP_EXECUTABLE and .HADOOP_STREAMING_JAR must both be set")
        if not os.path.exists(cls.HADOOP_EXECUTABLE):
            raise IOError("MapReduce.HADOOP_EXECUTABLE == \"%s\", which does not exist" % cls.HADOOP_EXECUTABLE)
        if not os.path.exists(cls.HADOOP_STREAMING_JAR):
            raise IOError("MapReduce.HADOOP_STREAMING_JAR == \"%s\", which does not exist" % cls.HADOOP_STREAMING_JAR)

    @classmethod
    def _hadoopCall(cls, stdin, *args):
        process = subprocess.Popen([cls.HADOOP_EXECUTABLE] + list(args), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        stdout, stderr = process.communicate(stdin)
        return process.returncode, stdout, stderr

    @classmethod
    def hadoopRemove(cls, hdfsPath):
        """Delete a file in HDFS.

        @type hdfsPath: string
        @param hdfsPath: The fileName path to delete.
        @raise IOError: If any I/O related error occurs, this function raises an error.
        """

        returncode, stdout, stderr = cls._hadoopCall(None, "fs", "-rmr", hdfsPath)
        if returncode != 0:
            raise IOError("Could not remove \"%s\": %s" % (hdfsPath, stderr))

    @classmethod
    def hadoopPopulate(cls, inputData, hdfsDirectory, fileName=None):
        """Populate an HDFS directory with data that will be used as
        input to a Hadoop workflow.

        This function is intended to be used in a distributed way:
        many Python processes can push data to the same HDFS directory
        simultaneously.

        @type inputData: list of Picklable Python objects
        @param inputData: The objects to send as a SequenceFile data stream.
        @type hdfsDirectory: string
        @param hdfsDirectory: Name of the HDFS directory to fill.  If it doesn't exist, it will be created.
        @type fileName: string or None
        @param fileName: Name of the output SequenceFile.  If no name is given, a name will be randomly chosen.
        @raise IOError: If any I/O related error occurs, this function raises an error.
        """

        cls._hadoopCheck()

        returncode, stdout, stderr = cls._hadoopCall(None, "fs", "-test", "-e", hdfsDirectory)
        if returncode != 0:
            returncode, stdout, stderr = cls._hadoopCall(None, "fs", "-mkdir", hdfsDirectory)
            if returncode != 0:
                raise IOError("Could not create directory \"%s\": %s" % (hdfsDirectory, stderr))

        else:
            returncode, stdout, stderr = cls._hadoopCall(None, "fs", "-test", "-d", hdfsDirectory)
            if returncode != 0:
                raise IOError("HDFS path \"%s\" is not a directory")

        if fileName is None:
            returncode = 0
            while returncode == 0:
                fileName = "".join(random.choice(string.ascii_uppercase + string.ascii_lowercase + string.digits) for x in xrange(20)) + ".sequencefile"
                returncode, stdout, stderr = cls._hadoopCall(None, "fs", "-test", "-e", "%s/%s" % (hdfsDirectory, fileName))

        else:
            returncode, stdout, stderr = cls._hadoopCall(None, "fs", "-test", "-e", "%s/%s" % (hdfsDirectory, fileName))
            if returncode == 0:
                returncode, stdout, stderr = cls._hadoopCall(None, "fs", "-rm", "%s/%s" % (hdfsDirectory, fileName))
                if returncode != 0:
                    raise IOError("Could not delete pre-existing file %s/%s" % (hdfsDirectory, fileName))

        process = subprocess.Popen([cls.HADOOP_EXECUTABLE, "jar", cls.HADOOP_STREAMING_JAR, "loadtb", "%s/%s" % (hdfsDirectory, fileName)], stdin=subprocess.PIPE)
        for datum in inputData:
            process.stdin.write(struct.pack("!bi", 0, 0))
            dataString = pickle.dumps(datum, protocol=PICKLE_PROTOCOL)
            process.stdin.write(struct.pack("!bi", 0, len(dataString)))
            process.stdin.write(dataString)
        process.stdin.close()

        returncode = process.wait()
        if returncode != 0:
            raise IOError("Could not create SequenceFile from typedbytes")
        
    @classmethod
    def hadoopGather(cls, hdfsDirectory):
        """Collect output from a Hadoop job.

        This function is intended to be used in a distributed way:
        many Python processes can pull data from the same HDFS
        directory simultaneously.

        @type hdfsDirectory: string
        @param hdfsDirectory: The name of the HDFS directory from which to collect data.
        @rtype: 2-tuple of list, dict
        @return: List of output records and dictionary of output key-value pairs.
        @raise IOError: If any I/O related error occurs, this function raises an error.
        """

        cls._hadoopCheck()

        returncode, stdout, stderr = cls._hadoopCall(None, "fs", "-ls", hdfsDirectory)
        if returncode != 0:
            raise IOError("Could not list directory \"%s\"" % hdfsDirectory)

        fileNames = []
        for line in stdout.strip().split(os.linesep):
            m = re.match(".*(%s/part-[0-9]{5})$" % hdfsDirectory, line)
            if m is not None:
                fileNames.append(m.group(1))

        outputRecords = []
        outputKeyValues = {}

        for fileName in fileNames:
            returncode, stdout, stderr = cls._hadoopCall(None, "jar", cls.HADOOP_STREAMING_JAR, "dumptb", fileName)
            if returncode != 0:
                raise IOError("Could not read SequenceFile \"%s\": %s" % (fileName, stderr))

            index = 0
            while index < len(stdout):
                header = stdout[index:(index + 5)]
                index += 5

                typecode, length = struct.unpack("!bi", header)
                if typecode != 0:
                    raise RuntimeError("Key should be binary, but typecode is %d" % typecode)

                key = stdout[index:(index + length)]
                index += length

                header = stdout[index:(index + 5)]
                index += 5

                typecode, length = struct.unpack("!bi", header)
                if typecode != 0:
                    raise RuntimeError("Value should be binary, but typecode is %d" % typecode)

                dataString = stdout[index:(index + length)]
                index += length

                record = pickle.loads(dataString)

                if len(key) == 0:
                    outputRecords.append(record)
                else:
                    outputKeyValues[key] = record

        return outputRecords, outputKeyValues

    def hadoopRun(self, inputHdfsDirectory, outputHdfsDirectory, iterationLimit=None, numberOfReducers=1, cmdenv=None, loggingLevel=logging.WARNING, overwrite=True, verbose=True):
        """Run the whole Hadoop workflow, which may involve multiple iterations of Hadoop jobs.

        @type inputHdfsDirectory: string
        @param inputHdfsDirectory: The name of the HDFS directory to use as input.  It should contain SequenceFiles generated by C{hadoopPopulate}.
        @type outputHdfsDirectory: string
        @param outputHdfsDirectory: The name of the HDFS directory to use as output.  If it exists and C{overwrite} is True, it will be overwritten.
        @type iterationLimit: int
        @param iterationLimit: The highest iteration to allow, with 1 being the first iteration.
        @type numberOfReducers: int
        @param numberOfReducers: Desired number of reducers.
        @type cmdenv: dict or None
        @param cmdenv: Environment variables to pass to the mapper and reducer processes.
        @type loggingLevel: logging level
        @param loggingLevel: The level of log output that will go to Hadoop's standard error.
        @type overwrite: bool
        @param overwrite: If C{outputHdfsDirectory} exists and this is True, the contents will be overwritten.
        @type verbose: bool
        @param verbose: If True, let Hadoop print its output to C{sys.stdout}.
        @rtype: 3-tuple of list, dict, int
        @return: List of output records, dictionary of output key-value pairs, and the final iteration number.
        @raise IOError: If any I/O related error occurs, this function raises an error.
        """

        self._hadoopCheck()

        returncode, stdout, stderr = self._hadoopCall(None, "fs", "-test", "-e", outputHdfsDirectory)
        if returncode == 0:
            if overwrite:
                returncode, stdout, stderr = self._hadoopCall(None, "fs", "-rmr", outputHdfsDirectory)
                if returncode != 0:
                    raise IOError("Could not remove path \"%s\": %s" % (outputHdfsDirectory, stderr))
            else:
                raise IOError("Directory \"%s\" already exists; remove it manually or pass overwrite=True" % outputHdfsDirectory)

        returncode, stdout, stderr = self._hadoopCall(None, "fs", "-mkdir", outputHdfsDirectory)
        if returncode != 0:
            raise IOError("Could not create directory \"%s\": %s" % (outputHdfsDirectory, stderr))

        baseOutputDirectory = outputHdfsDirectory

        iteration = 0
        while True:
            if iteration < len(self.chain):
                chain = self.chain[iteration]
            else:
                chain = self.chain[-1]

            outputHdfsDirectory = "%s/iteration-%d" % (baseOutputDirectory, iteration)

            outputRecords, outputKeyValues = self.hadoopIterate(inputHdfsDirectory, outputHdfsDirectory, iteration, numberOfReducers, cmdenv, loggingLevel, False, verbose)

            if self.done:
                break

            if chain:
                inputHdfsDirectory = outputHdfsDirectory

            iteration += 1
            if iterationLimit is not None and iteration >= iterationLimit:
                self.done = True
                break

        return outputRecords, outputKeyValues, iteration

    def hadoopIterate(self, inputHdfsDirectory, outputHdfsDirectory, iteration=0, numberOfReducers=1, cmdenv=None, loggingLevel=logging.WARNING, overwrite=True, verbose=True):
        """Run a Hadoop iteration, wait for it to finish, and collect the results.

        This function builds and submits mapper and reducer scripts
        containing the MapReduceApplication as a serialized class,
        which are unserialized remotely.

        @type inputHdfsDirectory: string
        @param inputHdfsDirectory: The name of the HDFS directory to use as input.  It should contain SequenceFiles generated by C{hadoopPopulate}.
        @type outputHdfsDirectory: string
        @param outputHdfsDirectory: The name of the HDFS directory to use as output.  If it exists and C{overwrite} is True, it will be overwritten.
        @type iteration: int
        @param iteration: The iteration number.
        @type numberOfReducers: int
        @param numberOfReducers: Desired number of reducers.
        @type cmdenv: dict or None
        @param cmdenv: Environment variables to pass to the mapper and reducer processes.
        @type loggingLevel: logging level
        @param loggingLevel: The level of log output that will go to Hadoop's standard error.
        @type overwrite: bool
        @param overwrite: If C{outputHdfsDirectory} exists and this is True, the contents will be overwritten.
        @type verbose: bool
        @param verbose: If True, let Hadoop print its output to C{sys.stdout}.
        @rtype: 2-tuple of list, dict
        @return: List of output records and dictionary of output key-value pairs.
        @raise IOError: If any I/O related error occurs, this function raises an error.
        """

        self._hadoopCheck()

        if overwrite:
            returncode, stdout, stderr = self._hadoopCall(None, "fs", "-test", "-e", outputHdfsDirectory)
            if returncode == 0:
                returncode, stdout, stderr = self._hadoopCall(None, "fs", "-rmr", outputHdfsDirectory)
                if returncode != 0:
                    raise IOError("Could not remove path \"%s\": %s" % (outputHdfsDirectory, stderr))
                
        if iteration < len(self.mapRedApps):
            mapRedApp = self.mapRedApps[iteration]
            mapRedAppSerialized = self.mapRedAppsSerialized[iteration]
        else:
            mapRedApp = self.mapRedApps[-1]
            mapRedAppSerialized = self.mapRedAppsSerialized[-1]

        gatherOutput = mapRedApp.gatherOutput
        imports = mapRedApp.imports
        if imports is None:
            imports = {}
        namespace = self._buildNamespace(imports)

        files = mapRedApp.files
        if files is None:
            files = []
        if cmdenv is None:
            cmdenv = {}

        self.done = False
        startMetadata = copy.deepcopy(self.metadata)

        overrideAttributes = {"metadata": startMetadata, "iteration": iteration}
        overrideAttributesString = pickle.dumps(overrideAttributes, protocol=PICKLE_PROTOCOL)

        template = os.path.join(os.path.split(__file__)[0], "MapReduceTemplate.py")
        if not os.path.exists(template):
            raise IOError("Could not find %s in the Augustus distribution" % template)
        template = open(template).read()

        application = os.path.join(os.path.split(__file__)[0], "MapReduceApplication.py")
        if not os.path.exists(application):
            raise IOError("Could not find %s in the Augustus distribution" % application)
        application = open(application).read()

        mapperScript = tempfile.NamedTemporaryFile(delete=False)
        mapperScript.write(template)
        mapperScript.write(application)

        for name, value in imports.items():
            if value is None:
                mapperScript.write("import %s%s" % (name, os.linesep))

            elif isinstance(value, basestring):
                mapperScript.write("import %s as %s%s" % (name, value, os.linesep))

            else:
                if hasattr(value, "items"):
                    for n, v in value.items():
                        mapperScript.write("from %s import %s as %s%s" % (name, n, v, os.linesep))
                else:
                    for v in value:
                        mapperScript.write("from %s import %s%s" % (name, v, os.linesep))

        mapperScript.write("sys.stdout = sys.stderr%s" % os.linesep)
        mapperScript.write("logging.basicConfig(level=%d)%s" % (loggingLevel, os.linesep))
        mapperScript.write("logger = logging.getLogger(\"%s\")%s" % (self.loggerName, os.linesep))

        mapperScript.write("overrideAttributesString = %r%s" % (overrideAttributesString, os.linesep))
        mapperScript.write("overrideAttributes = pickle.loads(overrideAttributesString)%s" % os.linesep)
        mapperScript.write("overrideAttributes[\"emit\"] = emit%s" % os.linesep)
        mapperScript.write("overrideAttributes[\"logger\"] = logging.getLogger(\"%s\")%s" % (mapRedApp.loggerName, os.linesep))

        mapperScript.write("class FakePerformanceTable(object):%s" % os.linesep)
        mapperScript.write("    def __init__(self):%s" % os.linesep)
        mapperScript.write("        pass%s" % os.linesep)
        mapperScript.write("    def __repr__(self):%s" % os.linesep)
        mapperScript.write("        return \"<FakePerformanceTable at 0x%%x>\" %% id(self)%s" % os.linesep)
        mapperScript.write("    def absorb(self, performanceTable):%s" % os.linesep)
        mapperScript.write("        pass%s" % os.linesep)
        mapperScript.write("    def begin(self, key):%s" % os.linesep)
        mapperScript.write("        pass%s" % os.linesep)
        mapperScript.write("    def end(self, key):%s" % os.linesep)
        mapperScript.write("        pass%s" % os.linesep)
        mapperScript.write("    def pause(self, key):%s" % os.linesep)
        mapperScript.write("        pass%s" % os.linesep)
        mapperScript.write("    def unpause(self, key):%s" % os.linesep)
        mapperScript.write("        pass%s" % os.linesep)
        mapperScript.write("    def block(self):%s" % os.linesep)
        mapperScript.write("        pass%s" % os.linesep)
        mapperScript.write("    def unblock(self):%s" % os.linesep)
        mapperScript.write("        pass%s" % os.linesep)
        mapperScript.write("overrideAttributes[\"performanceTable\"] = FakePerformanceTable()%s" % os.linesep)

        mapperScript.write("mapRedAppSerialized = %r%s" % (mapRedAppSerialized, os.linesep))
        mapperScript.write("appClass = unserializeClass(mapRedAppSerialized, MapReduceApplication, overrideAttributes, globals())%s" % os.linesep)

        mapperScript.write("controller = Controller(appClass())%s" % os.linesep)
        mapperScript.write("controller.mapper()%s" % os.linesep)
        mapperScript.close()

        reducerScript = tempfile.NamedTemporaryFile(delete=False)
        reducerScript.write(template)
        reducerScript.write(application)

        for name, value in imports.items():
            if value is None:
                reducerScript.write("import %s%s" % (name, os.linesep))

            elif isinstance(value, basestring):
                reducerScript.write("import %s as %s%s" % (name, value, os.linesep))

            else:
                if hasattr(value, "items"):
                    for n, v in value.items():
                        reducerScript.write("from %s import %s as %s%s" % (name, n, v, os.linesep))
                else:
                    for v in value:
                        reducerScript.write("from %s import %s%s" % (name, v, os.linesep))

        reducerScript.write("sys.stdout = sys.stderr%s" % os.linesep)
        reducerScript.write("logging.basicConfig(level=%d)%s" % (loggingLevel, os.linesep))
        reducerScript.write("logger = logging.getLogger(\"%s\")%s" % (self.loggerName, os.linesep))

        reducerScript.write("overrideAttributesString = %r%s" % (overrideAttributesString, os.linesep))
        reducerScript.write("overrideAttributes = pickle.loads(overrideAttributesString)%s" % os.linesep)
        reducerScript.write("overrideAttributes[\"emit\"] = emit%s" % os.linesep)
        reducerScript.write("overrideAttributes[\"logger\"] = logging.getLogger(\"%s\")%s" % (mapRedApp.loggerName, os.linesep))

        reducerScript.write("class FakePerformanceTable(object):%s" % os.linesep)
        reducerScript.write("    def __init__(self):%s" % os.linesep)
        reducerScript.write("        pass%s" % os.linesep)
        reducerScript.write("    def __repr__(self):%s" % os.linesep)
        reducerScript.write("        return \"<FakePerformanceTable at 0x%%x>\" %% id(self)%s" % os.linesep)
        reducerScript.write("    def absorb(self, performanceTable):%s" % os.linesep)
        reducerScript.write("        pass%s" % os.linesep)
        reducerScript.write("    def begin(self, key):%s" % os.linesep)
        reducerScript.write("        pass%s" % os.linesep)
        reducerScript.write("    def end(self, key):%s" % os.linesep)
        reducerScript.write("        pass%s" % os.linesep)
        reducerScript.write("    def pause(self, key):%s" % os.linesep)
        reducerScript.write("        pass%s" % os.linesep)
        reducerScript.write("    def unpause(self, key):%s" % os.linesep)
        reducerScript.write("        pass%s" % os.linesep)
        reducerScript.write("    def block(self):%s" % os.linesep)
        reducerScript.write("        pass%s" % os.linesep)
        reducerScript.write("    def unblock(self):%s" % os.linesep)
        reducerScript.write("        pass%s" % os.linesep)
        reducerScript.write("overrideAttributes[\"performanceTable\"] = FakePerformanceTable()%s" % os.linesep)

        reducerScript.write("mapRedAppSerialized = %r%s" % (mapRedAppSerialized, os.linesep))
        reducerScript.write("appClass = unserializeClass(mapRedAppSerialized, MapReduceApplication, overrideAttributes, globals())%s" % os.linesep)

        reducerScript.write("controller = Controller(appClass())%s" % os.linesep)
        reducerScript.write("controller.reducer()%s" % os.linesep)
        reducerScript.close()

        if verbose:
            stdout = None
        else:
            stdout = subprocess.PIPE

        fileargs = ["-file"] * (2 * len(files))
        fileargs[1::2] = files

        envargs = ["-cmdenv"] * (2 * len(cmdenv))
        envargs[1::2] = ["%s=%s" % (n, v) for n, v in cmdenv.items()]

        process = subprocess.Popen([self.HADOOP_EXECUTABLE, "jar", self.HADOOP_STREAMING_JAR, "-D", "mapred.reduce.tasks=%d" % numberOfReducers, "-D", "stream.map.output=typedbytes", "-D", "stream.reduce.input=typedbytes", "-D", "stream.reduce.output=typedbytes", "-inputformat", "org.apache.hadoop.mapred.SequenceFileAsBinaryInputFormat", "-outputformat", "org.apache.hadoop.mapred.SequenceFileOutputFormat", "-input", "%s/*" % inputHdfsDirectory, "-output", outputHdfsDirectory, "-mapper", mapperScript.name, "-reducer", reducerScript.name, "-file", mapperScript.name, "-file", reducerScript.name] + fileargs + envargs, stdout=stdout)
        process.wait()

        try:
            os.remove(mapperScript.name)
        except OSError:
            pass
        try:
            os.remove(reducerScript.name)
        except OSError:
            pass

        if process.returncode != 0:
            raise RuntimeError("Hadoop streaming failed")

        if gatherOutput:
            outputRecords, outputKeyValues = self.hadoopGather(outputHdfsDirectory)

            overrideAttributes = {"metadata": startMetadata, "iteration": iteration, "emit": None, "logger": logging.getLogger(mapRedApp.loggerName)}

            appClass = unserializeClass(mapRedAppSerialized, MapReduceApplication, overrideAttributes, namespace)
            appInstance = appClass()

            if appInstance.endIteration(outputRecords, outputKeyValues):
                self.done = True

            self.metadata = appInstance.metadata

            return outputRecords, outputKeyValues

        else:
            self.metadata = startMetadata
            return [], {}
