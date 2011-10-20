from tools import Timer
from datasource.db import DBUtility
from tools import LogUtil

class FlightScan:
    
    def __init__(self, config):
        self.config = config
        self.logger = LogUtil.Logging.getLogger()

    
    def start(self, interval = 600):
        pass
        #self.timer = Timer.Timer(interval, self.do)
        #self.timer.start()
        
    
    def do(self):
        pass
    
        
        
    