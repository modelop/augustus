#!/usr/bin/env python2.3

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
import xml_fifo_io2
import time
import re
import sys
import random

#----------------------------------------------------------------------

def main():
    """handle user command when run as top level program"""
    from optparse import OptionParser, make_option

    usage = 'usage: %prog [options] event_files (default "events.xml")'
    version = "%prog 0.0 alpha"
    option_list = [
      make_option('-f','--fifo',metavar='FIFO',default='event.fifo',help='write events to FIFO (default "event.fifo")'),
      make_option('-z','--gunzip',action='store_true',help='unzip input file while reading'),
      make_option('-d','--debug_out',metavar='debug_out',default=None,help='If set, a file with events that were read'),
    ]
    parser = OptionParser(usage=usage,version=version,option_list=option_list)
    (opt,args) = parser.parse_args()

    if (opt.debug_out is not None):
        try:
            debug_out=file(opt.debug_out,'wb')
        except:
            print 'Failure to open debug file requested for event source.\n'
    else:
        debug_out=None

    if not args:
        args = ["events.xml"]

    for arg in args:
        if arg == '-':
            events_file = sys.stdin
        elif opt.gunzip:
            import gzip
            events_file = gzip.open(arg, "r")
        else:
            events_file = open(arg, "r")
        write_events(opt.fifo, events_file,debug_out)

#----------------------------------------------------------------------

def write_events (event_fifo_name, events_file,_debug_out):
    """Read from a file containing mission events and send them to a named
    fifo as events"""

    event_count = 0

    try:
        event_fifo = open (event_fifo_name, "w", 0)
    except:
        print "Unable to open event.fifo for write\n"
    else:
        try:

            while True:
                
                event_line = events_file.readline ()
                #print event_line
                #print event_line
                #_x=input('continue')
                if len (event_line) > 0:
                    if (event_line[:9] == "<mission>" or
                        event_line[:9] == "<mission " or 
                        event_line[:15] == "<scoring_event>"
                    ):
                        try:
                            event_line.index('xmlns')
                        except ValueError:
                            event_line=event_line.replace('>',' xmlns="mynamespace">',1)
                        event_count += 1
                        event_str = event_line
                    else:
                        event_str += event_line
                    
                    if ("</mission>" in event_line or
                        "</scoring_event>" in event_line
                    ):
                        current_event = "<EVENT>" + event_str + "</EVENT>"
                        while True:
                            #if random.randint (0, 100) > 98:
                            if random.randint (0, 100) > 101:
                                # hang up the fifo and re-open periodically to test reconnect
                                # behavior
                                #print 'CLOSED!',event_count
                                event_fifo.close ()
                                event_fifo = open (event_fifo_name, "w", 0)
                                #print 'RE-OPENED'
                                            
                            try:
                                xml_fifo_io2.send_text (event_fifo, current_event)
                                if (event_count > 0 and ((event_count % 3000) == 0)):
                                    print str(event_count) + " events sent\n"
                                break
                            except IOError:
                                event_fifo = open (event_fifo_name, "w", 0)

        except IOError:            
            pass

        events_file.close ()
        if (debug_out is not None):
            debug_out.close()
                    
                

#            print random.randint (0, 100)
#            # randomly hangup every so often to test connection re-acquire
#            if random.randint (0, 100) > 50:
#                print "hanging up the fifo and re-acquiring"
#                event_fifo.close ()
#                event_fifo = open (event_fifo_name, "w", 0)
#            else:
#                try:
#                    event_line = events_file.readline()
#                except:
#                    print "Failed to read from events_file\n"
#                else:
#                    try:
#                        # while there are lines in the file, look for mission
#                        # lines, then wrap the mission text in an <event> tag
#                        # and send it to the event queue.
#                        while (len(event_line) > 0):
#                            #if the current line is a mission
#                            if ((event_line[:9] == "<mission>") or (event_line[:15] == "<scoring_event>") or
#                                (event_line[:9] == "<mission ")):
#                                while (not "</mission>" in event_line) and (not "</scoring_event>" in event_line):
#                                    try:
#                                        event_line+=events_file.readline()
#                                    except:
#                                        print 'Could not find completion of event ',event_count
#                                event_count += 1
#                                if (event_count > 0 and ((event_count % 3000) == 0)):
#                                    print str(event_count) + " events sent\n"
#                                current_event = "<EVENT>" + event_line + "</EVENT>"
#                                while 1:
#                                    try:
#                                        xml_fifo_io2.send_text(event_fifo,current_event)
#                                        break
#                                    except IOError:
#                                        event_fifo=open(event_fifo_name,"w")
#                            # read the next line
#                            event_line = events_file.readline ()
#                    except IOError:
#                        print "Failed to read from events_file\n"
#                events_file.close ()

        # let the event handler know that we're done.
        #try:
        #    shutdown_message = "<shutdown></shutdown>"
        #    xml_fifo_io2.send_text(event_fifo, shutdown_message)
        #except Exception, e:
        #    sys.excepthook(sys.exc_info()[0],sys.exc_info()[1], sys.exc_info()[2])

        try:
            event_fifo.close ()
        except Exception, e:
            sys.excepthook(sys.exc_info()[0],sys.exc_info[1],sys.exc_info[2])
        print "write_events exiting after %d events\n" % event_count


#----------------------------------------------------------------------
# main

if __name__ == "__main__":
    main()

#################################################################
# vim:sw=4:sts=4:expandtab:shiftround
