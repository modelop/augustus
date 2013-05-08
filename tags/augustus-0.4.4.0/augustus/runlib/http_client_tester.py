#!/usr/bin/env python
"""

  Augustus httpd test client

"""

__copyright__ = """
Copyright (C) 2005-2006  Open Data ("Open Data" refers to
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

import sys
import urllib
import httplib

URL = "localhost: 8000"

def main():
    payload = (len(sys.argv) > 1 and sys.argv[1]) or "<xml>data</xml>"
    
    params = urllib.urlencode({'payload':payload})
    headers = {"Content-type":"application/x-www-form-urlencoded",
              "Accept":"text/plain"}
    conn = httplib.HTTPConnection(URL)
    
    print "sending payload=%s to /allcap" % payload
    conn.request("POST", "/allcap", params, headers)
    response = conn.getresponse()
    print "server says : ", response.status, response.reason
    data = response.read()
    print "got %s" % urllib.unquote(data)
    
    print "sending payload=%s to /firstcap" % payload
    conn.request("POST", "/firstcap", params, headers)
    response = conn.getresponse()
    print "server says : ", response.status, response.reason
    data = response.read()
    print "got %s" % urllib.unquote(data)
    
    print "sending payload=%s to /allcap, expecting error" % payload
    conn.request("POST", "/allcap", params, headers)
    response = conn.getresponse()
    print "server says : ", response.status, response.reason
   # data = response.read()
   # print "got %s" % urllib.unquote(data)
    
    conn.close()
    
if __name__ == "__main__":
    main()
