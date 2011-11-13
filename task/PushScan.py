# coding=utf-8

from tools import Timer
from tools import LogUtil
from apns import APNs
from apns import Payload
import time


class PushScan:
    PUSH_POINT_1 = "2_hours_before_takeoff"
    PUSH_POINT_2 = "1_hour_before_takeoff"
    PUSH_POINT_3 = "actual_takeoff"
    PUSH_POINT_4 = "30_minutes_before_arrival"
    PUSH_POINT_5 = "actual_arrival"
    
    
    def __init__(self, config, data_source):
        self.config = config
        self.logger = LogUtil.Logging.getLogger()
        self.data_source = data_source
        self.apns = APNs(use_sandbox = False, cert_file = self.config.cert_file, key_file = self.config.key_file)

    
    def start(self):
        self.timer = Timer.Timer(self.config.push_scan_interval, self.do)
        self.timer.start()
        
    
    def do(self):
        self.logger.info("push task start...")
        
        push_list = self.data_source.getPushCandidate()
        
        if push_list is not None:
            for push_candidate in push_list:
                if push_candidate['full_info'] != -1:
                    self.data_source.getFlightRealtimeInfoFromDB(push_candidate)
                    
                    if self.checkPush(push_candidate):
                        payload = Payload(alert = push_candidate['push_content'], sound = "default")
                        self.apns.gateway_server.send_notification(push_candidate['device_token'], payload)
                        self.data_source.storePushInfo(push_candidate)
                        self.logger.info("push succ to %s" % (push_candidate['device_token']))
        
        self.logger.info("push task end...")
    
    
    def checkPush(self, push_candidate):
        cur_time = time.strftime("%H:%M", time.localtime())
        cur_hour = int(cur_time[:2])
        cur_minute = int(cur_time[3:]) + cur_hour * 60
            
        if push_candidate['full_info'] == 1:
            push_candidate['push_content'] = "[%s]\n从[%s]到[%s]\n已于[%s]到达" % (push_candidate['flight_no'].encode("utf-8"), 
                                                                     self.data_source.getAirportName(push_candidate['takeoff_airport'], 'zh').encode("utf-8"), 
                                                                     self.data_source.getAirportName(push_candidate['arrival_airport'], 'zh').encode("utf-8"), 
                                                                     push_candidate['actual_arrival_time'].encode("utf-8"))
            push_candidate['push_switch'] = 'off'
            if PushScan.PUSH_POINT_5 not in push_candidate['push_info']:
                push_candidate['push_info'].append(PushScan.PUSH_POINT_5)
                return True
            else:
                return False       
        
        estimate_arrival_time = ""
        if push_candidate['estimate_arrival_time'] != '--:--':
            estimate_arrival_hour = int(push_candidate['estimate_arrival_time'][:2])
            estimate_arrival_minute = int(push_candidate['estimate_arrival_time'][3:]) + estimate_arrival_hour * 60
            estimate_arrival_time = push_candidate['estimate_arrival_time']
        else:
            estimate_arrival_hour = int(push_candidate['schedule_arrival_time'][:2])
            estimate_arrival_minute = int(push_candidate['schedule_arrival_time'][3:]) + estimate_arrival_hour * 60
            estimate_arrival_time = push_candidate['schedule_arrival_time']
        
        if (estimate_arrival_minute - cur_minute) < 30:
            push_candidate['push_content'] = "[%s]\n从[%s]到[%s]\n预计于[%s]到达" % (push_candidate['flight_no'].encode("utf-8"), 
                                                                       self.data_source.getAirportName(push_candidate['takeoff_airport'], 'zh').encode("utf-8"), 
                                                                       self.data_source.getAirportName(push_candidate['arrival_airport'], 'zh').encode("utf-8"), 
                                                                       estimate_arrival_time.encode("utf-8"))
            if PushScan.PUSH_POINT_4 not in push_candidate['push_info']:
                push_candidate['push_info'].append(PushScan.PUSH_POINT_4)
                return True
            else:
                return False
       
        if push_candidate['actual_takeoff_time'] != '--:--':
            push_candidate['push_content'] = "[%s]\n从[%s]到[%s]\n已于[%s]起飞" % (push_candidate['flight_no'].encode("utf-8"), 
                                                                     self.data_source.getAirportName(push_candidate['takeoff_airport'], 'zh').encode("utf-8"), 
                                                                     self.data_source.getAirportName(push_candidate['arrival_airport'], 'zh').encode("utf-8"),
                                                                     push_candidate['actual_takeoff_time'].encode("utf-8"))
            if PushScan.PUSH_POINT_3 not in push_candidate['push_info']:
                push_candidate['push_info'].append(PushScan.PUSH_POINT_3)
                return True
            else:
                return False
        
        estimate_takeoff_time = ""
        if push_candidate['estimate_takeoff_time'] != '--:--':
            estimate_takeoff_hour = int(push_candidate['estimate_takeoff_time'][:2])
            estimate_takeoff_minute = int(push_candidate['estimate_takeoff_time'][3:]) + estimate_takeoff_hour * 60
            estimate_takeoff_time = push_candidate['estimate_takeoff_time']
        else:
            estimate_takeoff_hour = int(push_candidate['schedule_takeoff_time'][:2])
            estimate_takeoff_minute = int(push_candidate['schedule_takeoff_time'][3:]) + estimate_takeoff_hour * 60
            estimate_takeoff_time = push_candidate['schedule_takeoff_time']
            
        if (estimate_takeoff_minute - cur_minute) < 60:
            push_candidate['push_content'] = "[%s]\n从[%s]到[%s]\n预计于[%s]起飞" % (push_candidate['flight_no'].encode("utf-8"),
                                                                       self.data_source.getAirportName(push_candidate['takeoff_airport'], 'zh').encode("utf-8"), 
                                                                       self.data_source.getAirportName(push_candidate['arrival_airport'], 'zh').encode("utf-8"),
                                                                       estimate_takeoff_time.encode("utf-8"))
            if PushScan.PUSH_POINT_2 not in push_candidate['push_info']:
                push_candidate['push_info'].append(PushScan.PUSH_POINT_2)
                return True
            else:
                return False
        
        if (estimate_takeoff_minute - cur_minute) < 120:
            push_candidate['push_content'] = "[%s]\n从[%s]到[%s]\n预计于[%s]起飞" % (push_candidate['flight_no'].encode("utf-8"), 
                                                                       self.data_source.getAirportName(push_candidate['takeoff_airport'], 'zh').encode("utf-8"), 
                                                                       self.data_source.getAirportName(push_candidate['arrival_airport'], 'zh').encode("utf-8"),
                                                                       estimate_takeoff_time.encode("utf-8"))
            if PushScan.PUSH_POINT_1 not in push_candidate['push_info']:
                push_candidate['push_info'].append(PushScan.PUSH_POINT_1)
                return True
            else:
                return False
        
        return False
    
        
        
    