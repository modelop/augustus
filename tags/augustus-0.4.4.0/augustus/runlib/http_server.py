"""
  Augustus httpd request handler
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

import logging

from BaseHTTPServer import HTTPServer, BaseHTTPRequestHandler
import traceback


class HTTPInterfaceRequestHandler(BaseHTTPRequestHandler):
  """ Issue call to registered callback and return response """
    
  def do_GET(self):
    """Redirect mistaken GET attempts into a POST attempt."""
    self.do_POST()
    
  def do_POST(self):
    """Try to find the callback the request was looking for and pass the data onto that function."""
    myserver = self.server
    url = self.path
    self.log_debug("Got request: %s", url)

    callback = myserver.callback_map.get(url, None)
    if not callback:
      msg = "No callback for request: %s" % url
      self.log_error(msg)
      self.send_error(404)
    else:
      try:
        plen = int(self.headers.getheader('content-length'))
        data = self.rfile.read(plen) # Get request data
        response = callback(data)
        self.log_debug("response was %s", response)
        self.send_response(200)
        rlen = len(response)
        self.wfile.write("content-type: text/plain\ncontent-length:%d\n\n%s\n" % (rlen,response))
        self.wfile.close()
      except Exception,msg:
        try:
          self.log_error(traceback.format_exc())
          self.send_error(500)
        except:
          self.log_error("Error occured while trying to send error response.")

  #def log_request(self, code='-', size='-'):
    #"""TODO: Possibily override this functionality.  The inherited version of log_request passes the logging to log_message"""
    #pass

  def log_error(self, *args):
    """If the server has a logger, try to use that, otherwise send the message to the base class implementation."""
    if hasattr(self.server, "logger"):
      try:
        self.server.logger.error(args[0] % args[1:])
      except:
        self.server.logger.error(args[0])
    else:
      self.log_message(*args)
    
  def log_debug(self, format, *args):
    """If the server has a logger, try to use that, otherwise send the message to the base class implementation of log_message."""
    if hasattr(self.server, "logger"):
      try:
        self.server.logger.debug(format % args)
      except:
        self.BaseHTTPRequestHandler.log_message(format, *args)
    else:
      self.BaseHTTPRequestHandler.log_message(format, *args)

  def log_message(self, format, *args):
    """If the server has a logger, try to use that, otherwise send the message to the base class implementation."""
    if hasattr(self.server, "logger"):
      try:
        self.server.logger.info(format % args)
      except:
        self.BaseHTTPRequestHandler.log_message(format, *args)
    else:
      self.BaseHTTPRequestHandler.log_message(format, *args)


class HTTPInterfaceServer(HTTPServer):
  """ .. todo:: document HTTPInterfaceServer"""

  def __init__(self, address, request_handler = HTTPInterfaceRequestHandler, logger=None):
    """ Initalize the base server class and set up the logger.  Request_handler is the name of the class that should be created to handle requests.   """

    self.callback_map = {}
    HTTPServer.__init__(self, address, request_handler)
    if not logger:
      self.logger = logging.getLogger('httpd')
    else:
      self.logger = logger


  def register_callback(self, URL, callback):
    """ Register a callback to a url. """

    if not self.callback_map.has_key(URL):
      self.callback_map[URL] = callback
      return True
    else:
      self.logger.error("URL already mapped to callback (%s)", URL)
      return False

  def unregister_callback(self, URL):
    """ Remove a registered url. """

    if not self.callback_map.has_key(URL):
      self.logger.error("No callback at %s", URL)
      return False
    else:
      del self.callback_map[URL]
      return True


if __name__ == '__main__':
    """ Test Code """
    def allcapitalize(string):
        return string.upper()
    def firstcapitalize(string):
        return string.capitalize()
        
    logging.basicConfig(level=logging.DEBUG)
    
    """Start a test server up on port 8000 and accept well scripted series of requests."""
    server_addr = ('',8000)
    server = HTTPInterfaceServer(server_addr)
    
    #Add a callback
    server.register_callback('/allcap', allcapitalize)
    print '   ', "Serving one request..."
    server.handle_request()
    #Try to add the same callback, this should generate an error but not crash.
    server.register_callback('/allcap', allcapitalize)
    
    #Remove the callback
    print '   ', "Removing /allcap"
    server.unregister_callback('/allcap')
    #Try removing it again, this should output an error but not crash.
    server.unregister_callback('/allcap')
    
    #Now add a new handler and checking for two more requests.
    #One request will try to access the now removed handler and the other will try the new handler.
    print '   ', "Adding /firstcap and serving twice"
    server.register_callback('/firstcap', firstcapitalize)
    server.handle_request()
    server.handle_request()

