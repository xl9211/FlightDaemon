# coding=utf-8

from tools import Timer
from tools import LogUtil
import time
import traceback


class FlightFixInfoScan:
    
    def __init__(self, config, data_source):
        self.config = config
        self.logger = LogUtil.Logging.getLogger()
        self.data_source = data_source
        
        self.do()

    
    def start(self):
        self.timer = Timer.Timer(self.config.flight_fix_scan_interval, self.do)
        self.timer.start()
        
    
    def do(self):
        try:
            cur_time = time.strftime("%H:%M", time.localtime())
            
            if not (cur_time >= self.config.stop_fly_start and cur_time <= self.config.stop_fly_end):
                return 0
            
            self.logger.info("overday flight fix info spider start...")
            
            cur_date = time.strftime("%Y-%m-%d", time.localtime())
            overday_route_list = self.data_source.getOverdayRoute(cur_date)
            
            if overday_route_list is not None:
                for overday_route in overday_route_list:
                    self.data_source.updateFlightFixInfo(overday_route['takeoff_airport'], overday_route['arrival_airport'])
            
            self.logger.info("overday flight fix info spider end...")
            
            return 0
        except:
            msg = traceback.format_exc()
            self.logger.error(msg)
            
            return None
    
        
        
    