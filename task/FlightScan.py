# coding=utf-8

from tools import Timer
from tools import LogUtil
import json


class FlightScan:
    
    def __init__(self, config, data_source):
        self.config = config
        self.logger = LogUtil.Logging.getLogger()
        self.data_source = data_source

    
    def start(self):
        self.timer = Timer.Timer(self.config.flight_scan_interval, self.do)
        self.timer.start()
        
    
    def do(self):
        self.logger.info("lived flight realtime info spider start...")
        
        lived_flight_list = self.data_source.getAllLivedFlight()
        self.logger.info("lived flight %s" % (json.dumps(lived_flight_list)))
        
        if lived_flight_list is not None:
            for lived_flight in lived_flight_list:
                self.data_source.getFlightRealtimeInfo(lived_flight, True, False)
        
        self.logger.info("lived flight realtime info spider end...")
    
        
        
    