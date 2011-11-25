# coding=utf-8

from tools import LogUtil
from apns import APNs
from apns import Payload
import datetime
import traceback
from tools import BGTask

PUSH_TASK_QUEUE = BGTask.getBGQueue('push_task_queue', 10)

class PushTask:
    PUSH_POINT_1 = "2_hours_before_takeoff"
    PUSH_POINT_2 = "1_hour_before_takeoff"
    PUSH_POINT_3 = "actual_takeoff"
    PUSH_POINT_4 = "30_minutes_before_arrival"
    PUSH_POINT_5 = "actual_arrival"
    
    
    def __init__(self, config, data_source):
        self.config = config
        self.logger = LogUtil.Logging.getLogger()
        self.data_source = data_source
        
    
    @PUSH_TASK_QUEUE.task
    def do(self, flight):
        try:
            self.logger.info("push task start...")
            
            push_list = self.data_source.getPushCandidate(flight)
            apns = APNs(use_sandbox = False, cert_file = self.config.cert_file, key_file = self.config.key_file)
            
            if push_list is not None:
                for push_candidate in push_list:
                    if self.checkPush(push_candidate, flight):
                        self.logger.info("push to %s %s" % (flight['flight_no'].encode("utf-8"), push_candidate['device_token'].encode("utf-8")))
                        
                        payload = Payload(alert = push_candidate['push_content'], sound = "pushmusic.wav")
                        apns.gateway_server.send_notification(push_candidate['device_token'], payload)
                        
                        self.data_source.storePushInfo(push_candidate, flight)
                        self.logger.info("push succ to %s" % (push_candidate['device_token']))
            
            self.logger.info("push task end...")
        except:
            self.logger.error("%s %s" % (push_candidate['device_token'].encode("utf-8"), push_candidate['push_content']))
            msg = traceback.format_exc()
            self.logger.error(msg)
            
            return None
    
    
    def checkPush(self, push_candidate, flight):        
        cur_time = datetime.datetime.now()
            
        if flight['full_info'] == 1:
            push_candidate['push_content'] = "%s\n从 %s 到 %s\n已于 %s 到达" % (flight['flight_no'].encode("utf-8"), 
                                                                     self.data_source.getAirportName(flight['takeoff_airport'], 'zh').encode("utf-8"), 
                                                                     self.data_source.getAirportName(flight['arrival_airport'], 'zh').encode("utf-8"), 
                                                                     flight['actual_arrival_time'].encode("utf-8"))
            push_candidate['push_switch'] = 'off'
            if PushTask.PUSH_POINT_5 not in push_candidate['push_info']:
                push_candidate['push_info'].append(PushTask.PUSH_POINT_5)
                return True
            else:
                return False
              
        estimate_arrival_time = ""
        if flight['estimate_arrival_time'] != '--:--':
            estimate_arrival_time = flight['schedule_takeoff_date'] + " " + flight['estimate_arrival_time']
            estimate_arrival_time = datetime.datetime.strptime(estimate_arrival_time, "%Y-%m-%d %H:%M")
            if flight['estimate_arrival_time'] < flight['schedule_takeoff_time']:
                estimate_arrival_time += datetime.timedelta(1)
        else:        
            estimate_arrival_time = flight['schedule_takeoff_date'] + " " + flight['schedule_arrival_time']
            estimate_arrival_time = datetime.datetime.strptime(estimate_arrival_time, "%Y-%m-%d %H:%M")
            if flight['schedule_arrival_time'] < flight['schedule_takeoff_time']:
                estimate_arrival_time += datetime.timedelta(1)
        
        interval = estimate_arrival_time - cur_time
        
        if interval.days == 0 and interval.seconds < 1800 and flight['actual_takeoff_time'] != '--:--':
            push_candidate['push_content'] = "%s\n从 %s 到 %s\n预计于 %s 到达" % (flight['flight_no'].encode("utf-8"), 
                                                                       self.data_source.getAirportName(flight['takeoff_airport'], 'zh').encode("utf-8"), 
                                                                       self.data_source.getAirportName(flight['arrival_airport'], 'zh').encode("utf-8"), 
                                                                       estimate_arrival_time.strftime("%H:%M").encode("utf-8"))
            if PushTask.PUSH_POINT_4 not in push_candidate['push_info']:
                push_candidate['push_info'].append(PushTask.PUSH_POINT_4)
                return True
            else:
                return False
       
        if flight['actual_takeoff_time'] != '--:--':
            push_candidate['push_content'] = "%s\n从 %s 到 %s\n已于 %s 起飞\n预计于 %s 到达" % (flight['flight_no'].encode("utf-8"), 
                                                                     self.data_source.getAirportName(flight['takeoff_airport'], 'zh').encode("utf-8"), 
                                                                     self.data_source.getAirportName(flight['arrival_airport'], 'zh').encode("utf-8"),
                                                                     flight['actual_takeoff_time'].encode("utf-8"),
                                                                     estimate_arrival_time.strftime("%H:%M").encode("utf-8"))
            if PushTask.PUSH_POINT_3 not in push_candidate['push_info']:
                push_candidate['push_info'].append(PushTask.PUSH_POINT_3)
                return True
            else:
                return False
        
        estimate_takeoff_time = ""
        if flight['estimate_takeoff_time'] != '--:--':           
            estimate_takeoff_time = flight['schedule_takeoff_date'] + " " + flight['estimate_takeoff_time']
            estimate_takeoff_time = datetime.datetime.strptime(estimate_takeoff_time, "%Y-%m-%d %H:%M")
            if flight['estimate_takeoff_time'] < flight['schedule_takeoff_time']:
                estimate_arrival_time += datetime.timedelta(1)
        else:       
            estimate_takeoff_time = flight['schedule_takeoff_date'] + " " + flight['schedule_takeoff_time']
            estimate_takeoff_time = datetime.datetime.strptime(estimate_takeoff_time, "%Y-%m-%d %H:%M")
        
        interval = estimate_takeoff_time - cur_time
           
        if interval.days == 0 and interval.seconds < 3600:
            push_candidate['push_content'] = "%s\n从 %s 到 %s\n预计于 %s 起飞" % (flight['flight_no'].encode("utf-8"),
                                                                       self.data_source.getAirportName(flight['takeoff_airport'], 'zh').encode("utf-8"), 
                                                                       self.data_source.getAirportName(flight['arrival_airport'], 'zh').encode("utf-8"),
                                                                       estimate_takeoff_time.strftime("%H:%M").encode("utf-8"))
            if PushTask.PUSH_POINT_2 not in push_candidate['push_info']:
                push_candidate['push_info'].append(PushTask.PUSH_POINT_2)
                return True
            else:
                return False
        
        if interval.days == 0 and interval.seconds < 7200:
            push_candidate['push_content'] = "%s\n从 %s 到 %s\n预计于 %s 起飞" % (flight['flight_no'].encode("utf-8"), 
                                                                       self.data_source.getAirportName(flight['takeoff_airport'], 'zh').encode("utf-8"), 
                                                                       self.data_source.getAirportName(flight['arrival_airport'], 'zh').encode("utf-8"),
                                                                       estimate_takeoff_time.strftime("%H:%M").encode("utf-8"))
            if PushTask.PUSH_POINT_1 not in push_candidate['push_info']:
                push_candidate['push_info'].append(PushTask.PUSH_POINT_1)
                return True
            else:
                return False
        
        return False
    
        
        
    