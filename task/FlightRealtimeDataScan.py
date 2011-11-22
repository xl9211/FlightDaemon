# coding=utf-8

from tools import Timer
from tools import LogUtil
import json
import traceback
import PushTask


class FlightRealtimeDataScan:
    
    def __init__(self, config, data_source):
        self.config = config
        self.logger = LogUtil.Logging.getLogger()
        self.data_source = data_source
        
        self.push_task = PushTask.PushTask(self.config, self.data_source)

    
    def start(self):
        self.timer = Timer.Timer(self.config.flight_scan_interval, self.do)
        self.timer.start()
        
    
    def do(self):
        try:
            self.logger.info("lived flight realtime info spider start...")
            
            lived_flight_list = self.data_source.getAllLivedFlight()
            self.logger.info("lived flight %s" % (json.dumps(lived_flight_list)))
            
            if lived_flight_list is not None:
                for lived_flight in lived_flight_list:
                    ret = self.data_source.getFlightRealtimeInfo(lived_flight, True)
                    
                    if ret == 1:
                        self.push_task.do(lived_flight)
            
            self.logger.info("lived flight realtime info spider end...")
        except:
            msg = traceback.format_exc()
            self.logger.error(msg)
            
            return None
    
        
        
    