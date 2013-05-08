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

import augustus.core.xmlbase as xmlbase
import augustus.core.pmml41 as pmml
import augustus.core.config as config
import augustus.engine.mainloop
from augustus.core.defs import NameSpace, INVALID, MISSING

class DataStreamInterface(object):
    ### Overload the following:

    def initialize(self):
        pass

    def next(self):
        pass

    def get(self, field):
        return MISSING

    def flush(self):
        pass

class AugustusInterface(object):
    def __init__(self, configuration, dataStream=None, connect="", exceptions=True):
        if isinstance(configuration, config.AugustusConfiguration):
            configuration.validate(exception=True)
        else:
            try:
                configuration = xmlbase.loadfile(configuration, config.Config, lineNumbers=True)
            except IOError:
                configuration = xmlbase.load(configuration, config.Config)

        if configuration.exists(config.CustomProcessing):
            raise Exception("The Augustus class defines its own <CustomProcessing>; please leave this out of the configuration file.")

        self.dataStream = dataStream
        if configuration.child(config.DataInput).exists(config.Interactive):
            if dataStream is None:
                raise Exception("If the configuration has a DataInput <Interactive> block, then a DataStream object must be provided.")
        else:
            if dataStream is not None:
                raise Exception("If the configuration has no DataInput <Interactive> block, then a DataStream object must not be provided.")

        persistentStorage = config.PersistentStorage(connect=connect)
        persistentStorage.validate()
        customProcessing = config.CustomProcessing(persistentStorage)
        customProcessing.code = None
        customProcessing.callbackClass = self
        configuration.children.append(customProcessing)

        self.configuration = configuration
        self.mainLoop = augustus.engine.mainloop.MainLoop(self.configuration, rethrowExceptions=exceptions, dataStream=self.dataStream)

    def doBegin(self):
        """Run MainLoop.doBegin before any event records.  This implicitly calls self.begin."""
        self.mainLoop.doBegin()

    def doEvent(self):
        """Run MainLoop.doEvent on one event record.  This implicitly calls self.action."""
        self.mainLoop.doEvent()

    def doEnd(self):
        """Run MainLoop.doEnd after all event records (if there were no exceptions).  This implicitly calls self.end."""
        self.mainLoop.doEnd()

    def doShutdown(self):
        """Run MainLoop.doShutdown.  Should be called after all events, regardless of whether there were any exceptions (it closes all files and reports metadata)."""
        self.mainLoop.doShutdown()

    def run(self):
        """Run Augustus in the normal way: doBegin, loop over doEvent until StopIteration, then doEnd and doShutdown."""
        self.mainLoop.run()

    ### Overload the following:

    def begin(self, context):
        pass

    def action(self, context):
        pass

    def end(self, context):
        pass

    def exception(self, context):
        pass
