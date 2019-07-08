import sys
import datetime
import signal
from optparse import OptionParser
from PyQt5.QtCore import *
from PyQt5.QtGui import *
#from PyQt5.QtWebKitWidgets import *
from PyQt5.QtWebEngineWidgets import *
from PyQt5.QtWidgets import QApplication
from PyQt5.QtNetwork import QNetworkAccessManager, QNetworkRequest, QNetworkReply
from lxml import html
class MyNetworkAccessManager(QNetworkAccessManager):
 def __init__(self, url):
  QNetworkAccessManager.__init__(self)
  self.request = QNetworkRequest(QUrl(url))
  self.reply = self.get(self.request)
 def createRequest(self, operation, request, data):
  print ("mymanager handles ", request.url())
  str1="X-Content-Type-Options"
  str2="nosniff"
  str3="Content-Type"
  str4="text/html;charset=UTF-8"
#  b = bytearray()
  self.request.setRawHeader(str.encode(str1),str.encode(str2))
  self.request.setRawHeader(str.encode(str3),str.encode(str4))
  return QNetworkAccessManager.createRequest( self, operation, request, data )
#class Crawler( QWebPage ):
class Crawler( QWebEngineView ):
 def __init__(self, url, file):
  QWebEngineView.__init__( self )
#  QWebPage.__init__( self )
  self._url = url
  self._file = file
  manager = MyNetworkAccessManager(url)
  self.loadFinished.connect(manager, signal(QNetworkReply), SLOT(replyFinished(QNetworkReply)))
  #signal.signal( signal.SIGINT, signal.SIG_DFL )
  #self.loadFinished.connect(manager.finished(QNetworkReply))
  manager.get(QNetworkRequest(QUrl(url)));
  #self.setNetworkAccessManager(manager)
  #connect(manager, SIGNAL(authenticationRequired(QNetworkReply,QAuthenticator)), this, SLOT(authenticate(QNetworkReply,QAuthenticator)))
 def userAgentForUrl(self, url):
  return "Mozilla/122.0 (X11; Linux x86_64; rv:7.0.1) Gecko/20100101 Firefox/7.0.1"
 def crawl( self ):
  signal.signal( signal.SIGINT, signal.SIG_DFL )
  self.loadFinished.connect(self._finished_loading)
#  self.connect( self, SIGNAL( 'loadFinished(bool)' ), self._finished_loading )
  self.mainFrame().load( QUrl( self._url ) )
 def _finished_loading( self, result ):
  file = open( self._file, 'a' )
  result = self.mainFrame().toHtml()
  response = html.fromstring(str(result.encode('utf-8')))
  #file.write(str('Uttar Pradesh')+",")
  file.write(str(datetime.datetime.now())+",")
  finalResponse = response.xpath('//div[contains(@class,"gen_val1")]//text()')
  file.write(str(finalResponse[0]).lstrip().replace(',','')+",")
  finalResponse = response.xpath('//div[contains(@class,"gen_val2")]//text()')
  file.write(str(finalResponse[0]).lstrip().replace(',','')+",")
  finalResponse = response.xpath('//div[contains(@class,"gen_val3")]//text()')
  #file.write(str(finalResponse)+"\t")
  #finalResponse = response.xpath('//div[contains(@class,"portfolio_sub_value col-md-6 col-xs-6")]//text()')
  file.write(str(finalResponse[0]).lstrip().replace(',','')+"\n")
  file.close()
  sys.exit( 1 )

def main():
  app = QApplication(sys.argv)
  options = get_cmd_options()
  #crawler = Crawler( options.url, options.file )
  crawler = Crawler( 'http://meritindia.in/state-data/uttar-pradesh', '/home/abhishek/WebScraping/UP.csv' )
  crawler.crawl()
  sys.exit( app.exec_() )
  app.quit();

def get_cmd_options():
 return
 """
  gets and validates the input from the command line
 """
 usage = "usage: %prog [options] args"
 parser = OptionParser(usage)
 parser.add_option('-u', '--url', dest = 'url', help = 'URL to fetch data from')
 parser.add_option('-f', '--file', dest = 'file', help = 'Local file path to save data to')
 (options,args) = parser.parse_args()
 if not options.url:
  print ('You must specify an URL.,sys.argv[0],--help for more details')
  exit(1)
 if not options.file:
  print ('You must specify a destination file.,sys.argv[0],--help for more details')
  exit(1)
 return options

if __name__ == '__main__':
 main()
