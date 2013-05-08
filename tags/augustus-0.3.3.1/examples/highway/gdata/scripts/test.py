from augustus.kernel.unitable import *
import httplib
#import time
import random
random.seed()

def main():
  """Main program"""

  #Set up headers for our request and establish a connection to the server.
  headers = {"Content-type":"application/x-www-form-urlencoded","Accept":"text/plain"}
  conn=None
  try:
    conn = httplib.HTTPConnection('127.0.0.1',8000)
  except:
    print "Unable to connect to the server."
    return
  
  #Now read the events in from a file and send them to the server two at a time
  data = UniTable().from_csv_file('../data/test_data.csv')
  
  north_response = ""
  south_response = ""
  for row in data:
    payload = "<event><sensor>%s</sensor><volume>%d</volume></event>" % (row['sensor'],row['volume'])
    conn.request("POST", "/score", payload, headers)
    try:
      response = conn.getresponse()
      score = response.read()
    except:
      print "They are not responding to our hails Captain."
      return
    
    if "N" in score:
      north_response = score
    else:
      south_response = score
    
    write_web_page(north_response,south_response)

def write_web_page(north_response, south_response):
  out = open('../reports/status.html', 'w')
  page = '<html><head><title>Status</title></head><body>'
  
  if north_response:
    start = north_response.find("<Volume>")
    end = north_response.find("</Volume>")
    volume = int(north_response[start+8:end])
    if north_response.find("True") >= 0:
      #We have an alert
      page += '<p><div style="color:red">ALERT: Northbound volume is %d!</div></p>' % volume
    else:
      #No alert
      page += "<p>Ok: Northbound volume is %d.</p>" % volume
  
  if south_response:
    start = south_response.find("<Volume>")
    end = south_response.find("</Volume>")
    volume = int(south_response[start+8:end])
    if south_response.find("True") >= 0:
      #We have an alert
      page += '<p><div style="color:red">ALERT: Southbound volume is %d!</div></p>' % volume
    else:
      #No alert
      page += "<p>Ok: Southbound volume is %d.</p>" % volume
  
  page += '</body></html>'
  out.write(page)
  out.close()

#Run as main program
if __name__ == '__main__':
  main()