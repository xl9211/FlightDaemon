import urllib2
import traceback
from tools import LogUtil


class Spider:

    def __init__(self, config):
        self.timeout = 10
        self.headers = {
                        "User-Agent":"Mozilla/5.0 (Macintosh; Intel Mac OS X 10.7; rv:7.0.1) Gecko/20100101 Firefox/7.0.1",
                        "Accept":"text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                        "Accept-Language":"zh-cn,zh-tw;q=0.8,en-us;q=0.5,en;q=0.3",
                        "Accept-Charset":"ISO-8859-1,utf-8;q=0.7,*;q=0.7",
                        "Connection":"keep-alive",
                        "Cookie":"__utma=8610466.1209039631.1315038998.1319614239.1319617059.7; __utmz=8610466.1319618206.7.2.utmccn=(organic)|utmcsr=baidu|utmctr=%E5%87%86%E7%82%B9%E7%8E%87|utmcmd=organic; Clients_IPAddress=%u5317%u4EAC%u5E02%2C; ASPSESSIONIDQCTCADCC=PGCJFEIDCJKKHKKJNAEEKHPN; __utmc=8610466; ASPSESSIONIDACQQTDSB=MHBLBHIDLIMAEABIPIOCBCFA; __utmb=8610466"
                        }
        
        if config is not None:
            self.timeout = config.spider_timeout
        
        # Logger
        self.logger = LogUtil.Logging.getLogger()
    
    
    def fetch(self, url):
        try:
            self.logger.info("fetch url start %s" % (url))
            if url is not None:
                req = urllib2.Request(url = url, headers = self.headers)
                #response = urllib2.urlopen(self.url, timeout = self.timeout)
                response = urllib2.urlopen(req, timeout = self.timeout)
                self.logger.info("fetch url succ %s" % (url))
                return response.read()
            else:
                return -2
        except:
            self.logger.error("fetch url error %s" % (url))
            msg = traceback.format_exc()
            self.logger.error(msg)
            
            return -1