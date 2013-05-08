__copyright__ = """
Copyright (C) 2005-2007  Open Data ("Open Data" refers to
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

import struct
import os
import random
#import select
import time
import math

RandomBuggy = False
#RandomBuggy = True

class ReadOverflowError (RuntimeError):
  pass

MagicHeaderBegin = "\xF3\xFA\xE1\xE4"
MagicHeaderEnd = "\xF0\xF1\xDB\xE3"

##-----------------------------------------------------------------------------
#class WriteFifo:
#    """ Encapsulation of robust blob commmunication over a fifo. """
#
#    #-------------------------------------------------------------------------
#    def __init__ (self, fifoname):
#        self.fifoname = fifoname
#        self.outfile = file (self.fifoname, "wb", 0)
#
#    #-------------------------------------------------------------------------
#    def __del__ (self):
#        try:
#            self.outfile.close ()
#        except:
#            pass
#
#    #-------------------------------------------------------------------------
#    def send_text (text):
#    """Send the text to a file as a length delimited string"""
#        text_size = struct.pack ('!i', len (text))
#        write_text = MagicHeader + text_size + text
#        
#        while (1):
#            try:
#                if (RandomBuggy and (random.randint (0,100) > 0.98)):
#                     outfile.write (write_text[0:random.randint(1,len(write_text))])
#                else:
#                     outfile.write (write_text)
#                break
#            except IOError:
#                self.outfile = file (self.fifoname, "wb", 0)
#            
#            
##-----------------------------------------------------------------------------
#class ReadFifo:
#    """ Encapsulation of robust blob commmunication over a fifo. """
#
#    #-------------------------------------------------------------------------
#    def __init__ (self, fifoname):
#        self.fifoname = fifoname
#        self.infile = file (self.fifoname, "rb", 0)
#
#    #-------------------------------------------------------------------------
#    def __del__ (self):
#        try:
#            self.infile.close ()
#        except:
#            pass
#    
#    #-------------------------------------------------------------------------
#    def find_next_header ():
#    """Find the next header in the fifo. This would be a private function if
#    I knew how."""
#        try:
#            header = exact_size_read (infile, 4)
#            while (len (header) == 0):
#                header = exact_size_read (infile, 4)
#
##            stream_err = False
##            bytes_dropped = 0
#            while (header != MagicHeader):
##                print "Got an invalid header. It was %s instead of %s." % (header,MagicHeader)
##                stream_err = True
#                header = header[1:len(MagicHeader)]
#
#                while (len (header) < len(MagicHeader)):
#                    header = header + exact_size_read (infile, 1)
##                    bytes_dropped += 1
#
##            if (stream_err):
##                print "Stream Error. Skipped %d bytes before finding a valid record." % bytes_dropped
#        except IOError:
#            if (self.infile.closed)
#                self.infile = file (self.fifoname, "rb", 0)
#            else
#                raise
#
#        return True
#
#
#    #-------------------------------------------------------------------------
#    def receive_text ():
#    """Receive a length delimited blob of text from a file"""
#        expected_size = struct.calcsize ('!i')
#        good = False
#        stream_err = False
#        bytes_dropped = 0
#
#        while (1):
#            try:
#                find_next_header (infile)
#
#                size_struct = exact_size_read (infile, expected_size)
#                if (len (size_struct) != expected_size):
#                    continue
#
#                text_size, = struct.unpack ('!i', size_struct)
#
#                if (text_size > max_size):
#                    # if the size is too large, we're probably on an invalid header. Try to locate
#                    # the next field start delimiter to recover.
#                    print "Length header (%d) exceeds the maximum length allowed (%d) by xml_fifo_io" % \
#                        (text_size, max_size)
#                    print "Attempting to find the start of the next message...."
#                    continue
#
#                # read the block of text from the stream
#                text = exact_size_read (infile, text_size)
#                if (len (text) != text_size):
#                    if (not stream_err):
#                        stream_err = True
#                        print "Read a different size than specified by the header. Attempting to find the next valid record"
#                    bytes_dropped += len (text)
#                    continue
#
#                # If we reach here, then we found a valid record, as far as we know. Return it
#                # and let the reader verify the content.
#                break
#            except IOError:
#                if (self.infile.closed)
#                    self.infile = file (self.fifoname, "rb", 0)
#                else
#                    raise
#
#        if (stream_err):
#            print "Found next record. Dropped %d bytes." % bytes_dropped
#
#        return text

             

#-----------------------------------------------------------------------------
def send_text (outfile, text):
    """Send the text to a file as a length delimited string"""
    text_size = struct.pack ('!i', len (text))
    write_text = MagicHeaderBegin + text_size + text
    #print 'okay...'
    #print text
#   debugging aid. If random buggy is turned on, the write will behave
#   in a non-normal way part of the time.
    if (RandomBuggy and (random.randint (0,100) > 95)):
        ind = random.randint(1,len(write_text))
        if (random.randint (1,10) >= 6):
#            print "buggy %d %s" % (ind , text)
            outfile.write (write_text[0:ind])
        else:
            outfile.write (write_text[:ind])
            time.sleep (0.5)
            outfile.write (write_text[ind:])
    else:
        #print 'writing.'
        outfile.write (write_text)

    outfile.flush ()

#-----------------------------------------------------------------------------
def exact_size_read (infile, msglen):
    """ read full requested length form file """
    max_sleep = 20
    msg = ""
#    print "reading block of size %d" % msglen
    zero_reads = 0
    while (len (msg) != msglen):
        partial = infile.read (msglen - len (msg))
        if (len (partial) == 0):
            zero_reads += 1
#            print "  read %d" % 0
            sleep_time = 0.1 * (math.log (zero_reads + 1)/math.log(2))
            time.sleep (min((sleep_time, max_sleep)))
        else:
#            print "  read %d" % len(partial)
            msg = msg + partial
    
    return msg

#-----------------------------------------------------------------------------
# currently unused, but potential solution to corrupt size issues, if we
# have need of it.
def find_next_header2 (infile):
    
    expected_size = struct.calcsize ('!i') + 8

    header = exact_size_read (infile, expected_size)

    stream_err = False
    bytes_dropped = 0
    while (header[0::3] != MagicHeaderBegin or header[expected_size-4:] != MagicHeaderEnd):
#        print "Got an invalid header. It was %s instead of %s." % (header,MagicHeader)
        stream_err = True
        header = header[1:]

        while (len (header) < expected_size):
            header += exact_size_read (infile, 1)
            bytes_dropped += 1

    if (stream_err):
        print "Stream Error. Skipped %d bytes before finding a valid record." % bytes_dropped

    text_size, = struct.unpack ('!i', header[4:expected_size-5])
    return text_size

#-----------------------------------------------------------------------------
def find_next_header (infile, logger = None):

    header = exact_size_read (infile, 4)

    stream_err = False
    bytes_dropped = 0
    while (header != MagicHeaderBegin):
        if (stream_err == False and logger != None):
            errstr =  "Got an invalid header in xml_fifo_io. "
            errstr += "It was (%s) instead of (%s). " % (header, MagicHeaderBegin)
            errstr += "Skipping bytes to find the next record... "
            logger.error (errstr)
        stream_err = True
        header = header[1:]

        while (len (header) < len(MagicHeaderBegin)):
            header += exact_size_read (infile, 1)
            bytes_dropped += 1

    if (stream_err and logger != None):
        errstr =  "Skipped %d bytes before finding a valid record." \
            % bytes_dropped
        errstr += "It was (%s) instead of (%s) " % (header, MagicHeaderBegin)
        logger.error (errstr)

    return True

#-----------------------------------------------------------------------------
def receive_text (infile, max_size = 65536, logger = None):
    """Receive a length delimited blob of text from a file"""

    expected_size = struct.calcsize ('!i')
    good = False
    stream_err = False
    bytes_dropped = 0

    while (1):

        # find the next magic header
        find_next_header (infile)

        # read the size off the stream and unpack it
        size_struct = exact_size_read (infile, expected_size)
        text_size, = struct.unpack ('!i', size_struct)

        # if the size is too large, we're probably on an invalid header. 
        # Go back to the start and try to find the next valid header.
        if (text_size > max_size):
            if (logger != None):
                errstr =  "Length header (%d) " % text_size
                errstr += "exceeds the maximum length allowed "
                errstr += "(%d) by xml_fifo_io" % max_size
                logger.error (errstr)

                errstr = "Attempting to find the start of the next message...."
                logger.info (errstr)
            continue
        elif (text_size < 0):
            if (logger != None):
                errstr =  "Got a negative text size (%d) " % text_size
                errstr += "in xml_fifo_io"
                logger.error (errstr)
            continue

        # read the block of text from the stream
        text = exact_size_read (infile, text_size)
        # If we reach here, then we found a valid record, as far as we know.
        # Return it and let the reader verify the content.
        break

    if (stream_err):
        if (logger != None):
            errstr =  "Found next record in xml_fifo_io. "
            errstr += "Dropped %d bytes." % bytes_dropped
            logger.error (errstr)

    return text



