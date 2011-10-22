# coding=utf-8

from tools import Timer
from tools import LogUtil
import time
import json


class FlightScan:
    
    def __init__(self, config, data_source):
        self.config = config
        self.logger = LogUtil.Logging.getLogger()
        self.data_source = data_source

    
    def start(self):
        self.timer = Timer.Timer(self.config.scan_interval, self.do)
        self.timer.start()
        
    
    def do(self):
        self.logger.info("lived flight realtime info spider start...")
        
        cur_date = time.strftime("%Y-%m-%d", time.localtime())
        cur_time = time.strftime("%H:%M", time.localtime())
        cur_hour = int(cur_time[:2])
        cur_minute = int(cur_time[3:])
        cur_second = cur_hour * 60 * 60 + cur_minute * 60
        
        lived_flight_list = self.data_source.getAllLivedFlight(cur_date)
        self.logger.info("lived flight %s" % (json.dumps(lived_flight_list)))
        
        for lived_flight in lived_flight_list:
            hour = int(lived_flight['schedule_arrival_time'][:2])
            minute = int(lived_flight['schedule_arrival_time'][3:])
            second = hour * 60 * 60 + minute * 60

            if cur_second > second:
                self.data_source.getFlightRealtimeInfo(lived_flight)
        
        self.logger.info("lived flight realtime info spider end...")
    
        
        
    