# coding=utf-8

from tools import Timer
from tools import LogUtil
import json
import traceback
import PushScan


class FlightFixDataScan:
    
    def __init__(self, config, data_source):
        self.config = config
        self.logger = LogUtil.Logging.getLogger()
        self.data_source = data_source

    
    def start(self):
        self.timer = Timer.Timer(self.config.flight_scan_interval, self.do)
        self.timer.start()
        
    
    def do(self):
        try:
            pass
        except:
            msg = traceback.format_exc()
            self.logger.error(msg)
            
            return None
    
        
        
    