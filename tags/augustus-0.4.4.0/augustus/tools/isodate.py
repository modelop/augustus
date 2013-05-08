#!/usr/bin/env python

"""
isodate.py

Copyright 2002 Mark Nottingham, <mailto:mnot@pobox.com>

    THIS SOFTWARE IS SUPPLIED WITHOUT WARRANTY OF ANY KIND, AND MAY BE
    COPIED, MODIFIED OR DISTRIBUTED IN ANY WAY, AS LONG AS THIS NOTICE
    AND ACKNOWLEDGEMENT OF AUTHORSHIP REMAIN.

Functions for manipulating a subset of ISO8601 date, as specified by
  <http://www.w3.org/TR/NOTE-datetime>
  
Exposes:
  - parse(s)
    s being a conforming (regular or unicode) string. Raises ValueError for
    invalid strings. Returns a float (representing seconds from the epoch; 
    see the time module).
    
  - asString(i)
    i being an integer or float. Returns a conforming string.
  
.. todo:: Precision? it would be nice to have an interface that tells us how
          precise a datestring is, so that we don't make assumptions about it; 
          e.g., 2001 != 2001-01-01T00:00:00Z.
"""

import sys, time, re, operator
from types import StringType, UnicodeType, IntType, FloatType
from calendar import timegm


__version__ = "0.62"

date_parser = re.compile(r"""^
    (?P<year>\d{4,4})
    (?:
        -
        (?P<month>\d{1,2})
        (?:
            -
            (?P<day>\d{1,2})
            (?:
                T
                (?P<hour>\d{1,2})
                :
                (?P<minute>\d{1,2})
                (?:
                    :
                    (?P<second>\d{1,2})
                    (?:
                        \.
                        (?P<dec_second>\d+)?
                    )?
                )?                    
                (?:
                    Z
                )?
            )?
        )?
    )?
$""", re.VERBOSE)


def parse(s):
    """ parse a string and return seconds since the epoch. """
    assert type(s) in [StringType, UnicodeType]
    r = date_parser.search(s)
    print 'isodate ',r,s
    try:
        a = r.groupdict('0')
    except:
        raise ValueError, 'invalid date string format'
    d = timegm((   int(a['year']), 
                   int(a['month']) or 1, 
                   int(a['day']) or 1, 
                   int(a['hour']), 
                   int(a['minute']),
                   int(a['second']),
                   0,
                   0,
                   0
               ))
    return d - int("%s%s" % (
            a.get('tz_sign', '+'), 
            ( int(a.get('tz_hour', 0)) * 60 * 60 ) + \
            ( int(a.get('tz_min', 0)) * 60 ))
    )
    
def asString(i):
    """ given seconds since the epoch, return a dateTime string. """
    assert type(i) in [IntType, FloatType]
    year, month, day, hour, minute, second, wday, jday, dst = time.gmtime(i)
    o = str(year)
    if (month, day, hour, minute, second) == (1, 1, 0, 0, 0): return o
    o = o + '-%2.2d' % month
    if (day, hour, minute, second) == (1, 0, 0, 0): return o
    o = o + '-%2.2d' % day
    if (hour, minute, second) == (0, 0, 0): return o
    o = o + 'T%2.2d:%2.2d' % (hour, minute)
    if second != 0:
        o = o + ':%2.2d' % second
    o = o + 'Z'
    return o
def _cross_test():
    for iso in ("1997-07-16T19:20+01:00",
                "2001-12-15T22:43:46Z",
                "2004-09-26T21:10:15Z",
                "2004",
                "2005-04",
                "2005-04-30",
                "2004-09-26T21:10:15.1Z",
                "2004-09-26T21:10:15.1+05:00",
                "2004-09-26T21:10:15.1-05:00",
                ):
        timestamp = parse(iso)
        dt1 = datetime.datetime.utcfromtimestamp(timestamp)
        dt2 = parse_datetime(iso)
        if (dt1 != dt2 and
            dt1 != dt2.replace(microsecond=0)):
            raise AssertionError("Different: %r != %r" %
                                 (dt1, dt2))

if __name__ == "__main__":
    print parse("2005-05-16T03:09:11.487+00:00")
    #print parse("2005-05-16T03:09:11.487")
    print parse("1997-07-16T19:20+01:00")
    print parse("2001-12-15T22:43:46Z")
    print parse("2004-09-26T21:10:15Z")
    if _has_datetime:
        _cross_test()
        print parse_datetime("2005-05-16T03:09:11.487+00:00")
        print parse_datetime("2005-05-16T03:09:11.087+00:00")
        print parse_datetime("2005-05-16T03:09:11.487")
        print parse_datetime("1997-07-16T19:20+01:00")
        print parse_datetime("2001-12-15T22:43:46Z")
        print parse_datetime("2004-09-26T21:10:15Z")
