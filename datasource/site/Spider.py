import urllib2
import traceback
from tools import LogUtil


class Spider:

    def __init__(self):
        self.url = None
        self.content = None
        
        
        # Logger
        self.logger = LogUtil.Logging.getLogger()
    
    
    def fetch(self):
        try:
            self.logger.info("fetch url start %s" % (self.url))
            if self.url is not None:
                response = urllib2.urlopen(self.url)
                self.logger.info("fetch url succ %s" % (self.url))
                self.content = response.read()
                return 0
            else:
                return -2
        except:
            self.logger.error("fetch url error")
            msg = traceback.format_exc()
            self.logger.error(msg)
            
            return -1