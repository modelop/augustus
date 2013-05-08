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
import sys
import logging

#----------------------------------------------------------------------

def main():
    """handle user command when run as top level program"""
    from optparse import OptionParser, make_option

    usage = 'usage: %prog [options]'
    version = "%prog 0.0 alpha"
    option_list = [
      make_option('-f','--fifo',metavar='FIFO',default='score.fifo',help='read events from FIFO (default "score.fifo")'),
    ]

    parser = OptionParser(usage=usage,version=version,option_list=option_list)
    (opt,args) = parser.parse_args()
    logging.getLogger().setLevel(logging.INFO)
    log=logging
    
    read_scores(opt.fifo,log)    
#----------------------------------------------------------------------
def read_scores (score_fifo_name,log):
    """Read alerts from a fifo, count them, and drop them on the floor"""

    score_count = 0
    alert_total_size = 0

    try:
        score_fifo = open (score_fifo_name, "r")
    except:
        sys.excepthook(sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2])
    else:
		
        while 1:
            try:
                alert_text = xml_fifo_io2.receive_text(score_fifo)
            except IOError:
                print 'Error on fifo. Retrying'
                score_fifo = open (score_fifo_name, "r")
            except KeyboardInterrupt:
                print 'Ending.'
                sys.excepthook(sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2])
                break
            else:
                alert_tag = "<EVENT_SCORE>"
                alert_true_string = "<ALERT>True"
                if (alert_text[:len(alert_tag)] == alert_tag):
                    score_count += 1
                    if (alert_text.__contains__(alert_true_string)):
                        log.info("Alert noted by Scoring Engine")
                    if (score_count%250==0):
                        print "Score count ===================== "+str(score_count)
                elif (alert_text == "<shutdown></shutdown>"):
                    print "Shutting down cleanly on shutdown message"
                    break
        try:						
            score_fifo.close ()
        except Exception, e:
            sys.excepthook(sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2])

    print "read_scores exiting after %d scoring cycles\n" % score_count

#------------------------------------------------------------------
# main

if __name__ == "__main__":
    main()

#################################################################
# vim:sw=4:sts=4:expandtab:shiftround
