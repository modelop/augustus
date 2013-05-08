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

"""Defines classes to write out logfiles and metadata."""

import logging
import logging.handlers
import os
import sys
import time

from augustus.core import config
from augustus.core import xmlbase

########################################################### MetaDataLogger
class MetaDataLogger(logging.Logger):
    """A subclass Logger to collect and log segment and timing information.

    Public methods:

        __call__(self, message, function, *args, **kwargs)
        to_string(self)
        flush(self)

    Data attributes:

        data: A dictionary of segment information
    """
    def __init__(self, name):
        """Construct a MetaDataLogger.  Do not call this directly.

        Instead, use the following to create a MetaDataLogger:
        
            import logging
            # Save any existing custom logger class.
            OldClass = logging.getLoggerClass()

            # Set the custom class to this one.
            logging.setLoggerClass(MetaDataLogger)

            # The first call to getLogger with a new string will 
            # launch the current LoggerClass's constructor and
            # map the string to the newly created MetaDataLogger object.
            logging.getLogger('metadata')

            # Reset the original logger class.
            logging.setLoggerClass(OldClass)
        """
        self.timing = {}
        self.data = {}
        logging.Logger.__init__(self, name)

    def flush(self):
        """Write the contents of self.data and self.timing to log and clear them.
        """
        level = self.getEffectiveLevel()
        if level <= logging.INFO:
            self.info("### Current MetaData content ###")
            keys = self.timing.keys()
            keys.sort()
            for k in keys:
                v = self.data[k]
                self.info("%s : %s" %(str(k), str(v)))
                del self.data[k]

        if level <= logging.DEBUG:
            if level > logging.INFO:
                self.debug("### Current MetaData content ###")
            pairs = self.data.items()
            pairs.sort()
            for k, v in pairs:
                self.debug("%s : %s" %(str(k), str(v)))
        self.data = {}

    def startTiming(self, message):
        if self.getEffectiveLevel() <= logging.INFO:
            self.data.setdefault(message, 0)
            self.timing[message] = time.time()

    def stopTiming(self, message):
        """To be used in a pair after startTiming, with the same message.
        """
        if self.getEffectiveLevel() <= logging.INFO:
            if message not in self.data:
                message = "Metadata stop timing request for %s but no start request..." % message
                logging.getLogger().error(message)
            else:
                self.data[message] += time.time() - self.timing[message]
                # del self.timing[message]  # currently commented out to save a little


##################################################### Top level functions

def setupLogging(config_options=[]):
    """Set up overall logging for the program given the config options.

    Arguments:

        config_options (List of XML objects):
            Empty if the user did not elect to use logging.
            Otherwise could contain either or both of the
            <Logging>...</Logging> and <Metadata>...</Metadata>
            elements.
    """
    # There must be a metadata logger regardless of its level,
    # so that its internal functions may be called from any other
    # module without error.
    OldClass = logging.getLoggerClass()
    logging.setLoggerClass(MetaDataLogger)
    logging.getLogger("metadata")
    logging.setLoggerClass(OldClass)

    # this will be reset below if the loggers are configured.
    logger = logging.getLogger()
    metadata = logging.getLogger("metadata")
    logger.differentLevel = {}
    metadata.differentLevel = {}
    logger.naturalLevel = logging.CRITICAL
    metadata.naturalLevel = logging.CRITICAL
    logger.setLevel(level=logging.CRITICAL)
    metadata.setLevel(level=logging.CRITICAL)

    if not config_options:
        logging.disable(level=logging.CRITICAL)
    else:
        for child in config_options:
            if child.tag == "Logging":
                _setupLogging(child)
            elif child.tag == "Metadata":
                _setupLogging(child, "metadata")
            else:
                logging.getLogger("").critical("Logging configuration attempted for an object that is not a logger: %s" % str(child.tag))
                sys.exit()

def _setupLogging(config_options, logger=""):
    """Set up logging for the logger named ``logger``.

    Arguments:

        config_options (XML object, defined in xmlbase):
            The XML element <Logging>...</Logging> which contains the
            logging configuration settings, or <Metadata>...</Metadata>
            which duplicates the structure of Logging.

        logger (string):
            The name of the logger to be configured.
    """
    if config_options.exists(config.ToStandardOut):
        handler = logging.StreamHandler(sys.stdout)
#    #Currently not an option; check if this works before adding...
#    elif config_options.exists(config.ToHTTP):
#        #see: http://docs.python.org/howto/logging-cookbook.html
#        child = config_options.child(config.ToHTTP)
#        host = child["host"]
#        port =\
#            logging.handlers.DEFAULT_TCP_LOGGING_PORT if\
#            "port" not in child.attrib else child["port"]
#        handler = logging.handlers.SocketHandler(host, port)
    elif config_options.exists(config.ToLogFile):
        child = config_options.child(config.ToLogFile)
        file_name = child["name"]
        if len(child.children) == 0:
            # User wants to save to a single file
            mode = "w" if child.attrib.get("overwrite", False) else "a"
            handler = logging.FileHandler(file_name, mode)
        else:
            # User wants rotating log files
            attributes = child.attrib
            backup_count = 0 if "backupCount" not in attributes else\
                int(attributes["backupCount"])
            interval = 1 if "interval" not in attributes else\
                int(attributes["interval"])
            max_bytes = 0 if "maxBytes" not in attributes else\
                int(attributes["maxBytes"])
            mode = 'a' if "mode" not in attributes else attributes["mode"]
            utc = False if "utc" not in attributes else\
                int(attributes["utc"]) > 0
            when = 'H' if "when" not in attributes else attributes["when"]
            if child.exists(config.FileRotateBySize):
                handler =\
                    logging.handlers.RotatingFileHandler(
                        file_name,
                        mode=mode,
                        maxBytes=max_bytes,
                        backupCount=backup_count)
            else:  #xsd: must match "FileRotateByTime"
                handler =\
                    logging.handlers.TimedRotatingFileHandler(
                        file_name,
                        when=when,
                        interval=interval,
                        backupCount=backup_count,
                        utc=utc)
    else:
        # Default is to log to standard error.
        handler = logging.StreamHandler(sys.stderr)

    attributes = config_options.attrib
    #xsd: default format string and date format are defined in the XSD
    format_string =\
        "%(created)012.0f\t%(asctime)s\t%(levelname)s\t%(message)s" if\
        "formatString" not in attributes else attributes["formatString"]
    datefmt =\
        "%Y-%m-%dT%H:%M:%S" if "datefmt" not in attributes else\
        attributes["dateString"]
    formatter = logging.Formatter(format_string, datefmt=datefmt)
    handler.setFormatter(formatter)

    theLogger = logging.getLogger(logger)

    theLogger.addHandler(handler)
    level_choices = {"WARNING":logging.WARNING,
                     "INFO":logging.INFO,
                     "DEBUG":logging.DEBUG,
                     "ERROR":logging.ERROR}
    if logger=='':
        #xsd: root logger default level is logging.ERROR
        level = level_choices["ERROR" if "level" not in attributes else
                              attributes["level"]]
    else:
        #xsd: logger is 'metadata'; the only two options
        theLogger.propagate = 0
        level = level_choices["DEBUG" if "level" not in attributes else
                              attributes["level"]]
    theLogger.setLevel(level)
    theLogger.naturalLevel = level

    theLogger.differentLevel = {}
    for differentLevel in config_options.matches(config.DifferentLevel):
        stage = differentLevel.attrib["stage"]

        if stage == "segment":
            if "segment" not in theLogger.differentLevel:
                theLogger.differentLevel["segment"] = {}
            theLogger.differentLevel["segment"][differentLevel.attrib["segment"]] = differentLevel.attrib["level"]

        else:
            theLogger.differentLevel[stage] = differentLevel.attrib["level"]
