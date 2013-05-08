import collections
import logging
import math
import sys
import threading
import time
import xml
import xml.parsers.expat
from xml import sax

from augustus.core.defs import NameSpace, MISSING, INVALID
from augustus.kernel.unitable import UniTable

class DataStreamer:
    def __init__(self, errorCatcher=errorCatcher, fromHTTP=False, interactive=False, isXML=True, runForever=False, filename=None, **kwargs):
        self._errorCatcher = errorCatcher
        self._runOptions = NameSpace(fromHTTP=fromHTTP, interactive=interactive, isXML=isXML, runForever=runForever)
        self._fileList = filename  # None or else will become a list
        self.currentFileNumber = 0
        self._logger = logging.getLogger()
        self._logLevels = NameSpace(
            DEBUG=self._logger.getEffectiveLevel() <= logging.DEBUG,
            INFO=self._logger.getEffectiveLevel() <= logging.INFO,
            WARNING=self._logger.getEffectiveLevel() <= logging.WARNING,
            ERROR=self._logger.getEffectiveLevel() <= logging.ERROR)
        self._metadata = logging.getLogger('metadata')
        self._thread = None
        self._values = None
        self._tables = collections.deque()
        self._buffers = collections.deque()

        if interactive:
            self._reader = None
        elif fromHTTP:
            pass
        else:
            if filename == '-':
                self._fileList = ['-']
            else:
                import glob
                self._fileList = glob.glob(filename)
                self._fileList = sort()
                self._fileList.reverse()
                if len(self._fileList) == 0:
                    raise RuntimeError, "No Data Input files matched %s" % filename
        ## NOT DONE YET

    def get(self, field):
        if self._values is None:
            return MISSING
        elif field not in self._values.keys():
            self._logger.error("Data not found for field: %s" % field)
            return MISSING

        output = self._values[field]
        if isinstance(output, float) and (math.isnan(output) or math.isinf(output)):
            return INVALID
        return output

    def __iter__(self):
        return self

    def next(self):
        if self._values is not None:
            try:
                # Iterate over the Table.
                self._values = self._values.next()
                if self._logLevels.DEBUG:
                    self._logger.debug("This record: %s" % self._values)
                return self._values
            except StopIteration:
                self._values = None  # Reset.

        while self._values is None:
            # Try to get another Table.
            if len(self._tables) > 0:
                self._values = self._tables.popleft()
            elif self._thread and self._thread.is_alive():
                time.sleep(0)
            else:
                self._thread = None
                raise StopIteration
        self._values = self._values[0]

    def _readForever(self, BLOCKSIZE=4*(1024**2)):
        if self._runOptions.isXML:
            while True:
                currentBlock = os.read(fd, BLOCKSIZE)
                
            pass

        else:
            # FIXME: is this the correct indentation?
            while True:
                currentBlock = os.read(fd, BLOCKSIZE)
                if currentBlock == '':
                    time.sleep(0)
                self._buffers.append(StringIO.StringIO(currentBlock))
            # end FIXME

    def _readOnce(self):
        linesep = os.linesep
        while self._fileList:
            source = self._fileList.pop()
            filehandle = file(source)
            fd = f.fileno()
            while True:
                currentBlock = os.read(fd, BLOCKSIZE)
                if currentBlock == '':
                    self.currentFileNumber += 1
                self._buffers.append(StringIO.StringIO(currentBlock))

    def startReading(self):
        
        pass
        # If there is s
        # if Unitable:
        #   self._values = UniTable()
        # else:  # XML
        #   self._values = XMLTable()
        #   self._values.fromStream()

########################################################################## XMLTable
class XMLTable:
    """List of dictionaries intended to behave like a UniTable.

    Public Methods:
        __init__(self, table=[])
        clear(self)
        fromStream(self, stream)

    Example use:
        x = XMLTable()
        f = open("myfile.xml", 'r')
        x.fromStream(f)
        for row in x:
            for k,v in row.iteritems():
                print "k, v:", k, v
    """
    def __init__(self):
        self._currentRow = {}
        self._index = 0
        self._streamLoader = XMLStreamLoader()
        self._table = []

    def __iter__(self):
        return self

    def next(self):
        self._index += 1
        if self._index == len(self._table):
            raise StopIteration
        self._currentRow = self._table[self._index]
        return self

    def __getitem__(self, key):
        # Fails if the index exceeds table length or key not in dictionary.
        if isinstance(key, int):
            self._index = key
            self._currentRow = self._table[key]
            return self
        else:
            return self._currentRow[key]

    def __contains__(self, key):
        return key in self._currentRow

    def __repr__(self):
        return "<XMLTable row %d; %s >" % (self._index, ", ".join([":".join((k,v)) for k,v in self._currentRow.iteritems()]))

    def clear(self):
        self._table = []
        self._currentRow.clear()
        self._index = 0

    def fromStream(self, stream):
        self.clear()
        self._table = self._streamLoader.getTableFrom(stream)
        self._table.reverse()
        self._currentRow = self._table[self._index]

    def get(self, key):
        return self._currentRow[key]

    def iteritems(self):
        return self._currentRow.iteritems()

    def items(self):
        return self._currentRow.items()

    def keys(self):
        return self._currentRow.keys()


class XMLStreamLoader(xml.sax.handler.ContentHandler):
    """Loader for data of the following two formats:

    Wrapped; a whole file...
        <Table>
            eRow> <Col1>Value1</Col1> <Col2>Value2</Col2> </Row>
            <Row> <Col1>Value3</Col1> <Col2>Value4</Col2> </Row>
        </Table>

    Not wrapped; row-by-row...
        <Row> <Col1>Value1</Col1> <Col2>Value2</Col2> </Row>

    Attrs are not yet implemented:
        <Row Col1="Value1" Col2="Value2" />  # No; sorry...

    Result is a list of dictionaries; each dictionary represents a row:
        self._result = [{Col1:Value1, Col2:Value2}, {Col1:Value2, Col2:Value4}]
        self._result = [{Col1:Value1, Col2:Value2}]
    """
    def __init__(self):
        self._logger = logging.getLogger()
        self._parser = xml.sax.make_parser()
        self._parser.setContentHandler(self)

    def getTableFrom(self, stream):
        self._parser.reset()
        try:
            self._parser.parse(stream)
        except xml.sax.SAXParseException:
            self._logger.error("Bad data encountered in stream %s; discarding..." % stream)
        return self._result

    ################################### ContentHandler interface's required methods
    def startDocument(self):
        self._stack = [ (None, None, []), ]
        self._result = [{}]

    def endDocument(self):
        self._stack = None
        if len(self._result[-1]) == 0:
            self._result.pop()
        self._logger.debug("Finished reading %d rows of XML data" % len(self._result))

    def startElement(self, name, attrs):
        self._stack.append( (name, dict([(k,attrs[k]) for k in attrs.getNames()]), []) )

    def characters(self, content):
        content = str(content)
        if content.strip():
            # if not completely whitespace
            name, attr, children = self._stack[-1]
            if len(children) != 0 and isinstance(children[-1], basestring):
                children[-1] += content
            else:
                children.append(content)

    def endElement(self, name):
        currentName, attr, children = self._stack.pop()

        while name != currentName:
            self._logger.error("malformed data; unmatched tag: %s will be discarded" % currentName)
            if len(self.stack) > 1:
                currentName, attr, children = self._stack.pop()
            else:
                currentName, attr, children = name, {}, []

        self._result[-1].update(attr)

        if len(children) != 0:
            if isinstance(children[0], basestring):
                self._result[-1][currentName] = children[0]
            else:  # should never happen, because we are popping all elements
                pass
        else:  # ending a row
            if len(self._result[-1]) != 0:
                self._result.append({})
