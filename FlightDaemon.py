# coding=utf-8

import os
import sys

current_path = os.getcwd()
sys.path.append(current_path)

#import locale
#locale.setlocale(locale.LC_ALL, 'en_US.UTF-8')

import cherrypy
from tools import LogUtil
import traceback
from tools import Config
import json
import time
from datasource.DataSource import DataSource
from task.FlightRealtimeDataScan import FlightRealtimeDataScan
import random


from pygments import highlight #@UnresolvedImport
from pygments.lexers import PythonLexer #@UnresolvedImport
from pygments.formatters import HtmlFormatter #@UnresolvedImport

from mako.template import Template #@UnresolvedImport


class FlightDaemon:
    
    def __init__(self, config):
        self.config = config
        self.logger = LogUtil.Logging.getLogger()    
           
        self.data_source = DataSource(self.config)

        if self.config.debug_mode == 'no':
            self.flight_scan = FlightRealtimeDataScan(self.config, self.data_source)
            self.flight_scan.start()
        
        self.logger.info("Flight Daemon Started...")

    
    @cherrypy.expose
    def GST(self):
        try:
            self.logger.info("get request")
            
            code = []
            for thread_id, stack in sys._current_frames().items():
                code.append("\n# ThreadID: %s" % thread_id)
                for file_name, line_no, name, line in traceback.extract_stack(stack):
                    code.append("File: %s, line %d, in %s" % (file_name, line_no, name))
                    if line:
                        code.append(" %s" % (line.strip()))
                        
            return highlight("\n".join(code), PythonLexer(), HtmlFormatter(full = False, noclasses = True))
        except:
            msg = traceback.format_exc()
            self.logger.error(msg)
            
            return json.dumps(None)
        
        
    @cherrypy.expose
    def queryFlightInfoByFlightNO(self, flight_no, schedule_takeoff_date, lang = 'zh'):
        try:
            self.logger.info("get request %s %s %s" % (flight_no, schedule_takeoff_date, lang))
            
            flight_list = self.data_source.getFlightFixInfoByFlightNO(flight_no, schedule_takeoff_date)
 
            if flight_list == None:
                self.logger.info("fix data not exist")
                return json.dumps(None)
            if len(flight_list) == 0:
                self.logger.error("get fix data error")
                return json.dumps([])
            
            self.data_source.completeFlightInfo(flight_list, schedule_takeoff_date, lang)
  
            return json.dumps(flight_list) 
        except:
            msg = traceback.format_exc()
            self.logger.error(msg)
            
            return json.dumps(None)
        

    @cherrypy.expose
    def queryFlightInfoByRoute(self, takeoff_airport, arrival_airport, schedule_takeoff_date, company = 'all', lang = 'zh'):
        try:
            self.logger.info("get request %s %s %s %s %s" % (takeoff_airport, arrival_airport, schedule_takeoff_date, company, lang))
            
            flight_list = self.data_source.getFlightFixInfoByRoute(takeoff_airport, arrival_airport, schedule_takeoff_date, company)
 
            if flight_list == None:
                self.logger.info("fix data not exist")
                return json.dumps(None)
            if len(flight_list) == 0:
                self.logger.error("get fix data error")
                return json.dumps([])
            
            self.data_source.completeFlightInfo(flight_list, schedule_takeoff_date, lang)
  
            return json.dumps(flight_list)
        except:
            msg = traceback.format_exc()
            self.logger.error(msg)
            
            return json.dumps(None)
    

    @cherrypy.expose   
    def queryFlightInfoByRandom(self, lang = 'zh'):
        try:
            self.logger.info("get request %s" % (lang))
            
            right_flight = None
            cur_time = time.strftime("%H:%M", time.localtime())
            
            if not (cur_time > self.config.stop_fly_start and cur_time < self.config.stop_fly_end):
                random_list = self.data_source.getRandomFlightList(cur_time)
                flight_num = len(random_list)
                
                for i in range(0, flight_num): #@UnusedVariable
                    index = random.randint(0, len(random_list) - 1)
                    flight_no = random_list.pop(index)
                    
                    schedule_takeoff_date = time.strftime("%Y-%m-%d", time.localtime())
                    flights = json.loads(self.queryFlightInfoByFlightNO(flight_no, schedule_takeoff_date, lang))
                    
                    find = False
                    if flights is not None and len(flights) > 0:
                        for flight in flights:
                            if flight['flight_state'] == u'已经起飞':
                                right_flight = flight
                                find = True
                                break
                    
                    if find:
                        break
            
            # 如果没有正在飞行的航班
            if right_flight is None:
                flight = self.data_source.getRandomFlight()
                
                if flight is not None:
                    flights = json.loads(self.queryFlightInfoByFlightNO(flight['flight_no'], flight['schedule_takeoff_date'], lang))
                    
                    if flights is not None and len(flights) > 0:
                        right_flight = flights[0]

            if right_flight is None:
                return json.dumps([])
            
            flight_list = []
            flight_list.append(right_flight)

            return json.dumps(flight_list)             
        except:
            msg = traceback.format_exc()
            self.logger.error(msg)
            
            return json.dumps(None)
        
    
    @cherrypy.expose
    def updateFollowedFlightInfo(self, query_string, device_token = None, lang = 'zh'):
        try:
            self.logger.info("get request %s %s %s" % (query_string, str(device_token), lang))
            
            query_list = json.loads(query_string)
            flight_list = []
            
            if device_token is None:
                # 更新航班
                for flight in query_list:
                    tmp_list = self.data_source.getFlightFixInfoByUniq(flight['flight_no'], flight['takeoff_airport'], flight['arrival_airport'], flight['schedule_takeoff_date'])
     
                    if len(tmp_list) == 0:
                        self.logger.info("fix data not exist")
                        continue
                    if tmp_list == None:
                        self.logger.error("get fix data error")
                        continue
                
                    self.data_source.completeFlightInfo(tmp_list, flight['schedule_takeoff_date'], lang)
                    
                    tmp_flight = {}
                    tmp_flight['flight_state'] = tmp_list[0]['flight_state']
                    tmp_flight['estimate_takeoff_time'] = tmp_list[0]['estimate_takeoff_time']
                    tmp_flight['actual_takeoff_time'] = tmp_list[0]['actual_takeoff_time']
                    tmp_flight['estimate_arrival_time'] = tmp_list[0]['estimate_arrival_time']
                    tmp_flight['actual_arrival_time'] = tmp_list[0]['actual_arrival_time']
                    tmp_flight['flight_location'] = tmp_list[0]['flight_location']
                    tmp_flight['schedule_takeoff_time'] = tmp_list[0]['schedule_takeoff_time']
                    tmp_flight['schedule_arrival_time'] = tmp_list[0]['schedule_arrival_time']
                    tmp_flight['valid'] = tmp_list[0]['valid']
                    
                    flight_list.append(tmp_flight)
            else:
                # 关注航班
                self.data_source.storeFollowedInfo(device_token, query_list)

            return json.dumps(flight_list)
        except:
            msg = traceback.format_exc()
            self.logger.error(msg)
            
            return json.dumps(None)
        
        
    @cherrypy.expose
    def addFollowedFlightInfo(self, query_string, device_token):
        try:
            self.logger.info("get request %s %s" % (query_string, str(device_token)))
            
            if len(str(device_token)) >= 64:
                flight_list = json.loads(query_string)
                self.data_source.storeFollowedInfo(device_token, flight_list)
            
            ret = json.dumps(0)
            return ret
        except:
            msg = traceback.format_exc()
            self.logger.error(msg)
            
            return json.dumps(None)
        
    
    @cherrypy.expose
    def deleteFollowedFlightInfo(self, query_string, device_token):
        try:
            self.logger.info("get request %s %s" % (query_string, str(device_token)))
            
            flight_list = json.loads(query_string)
            self.data_source.deleteFollowedInfo(device_token, flight_list)
            
            ret = json.dumps(0)
            return ret
        except:
            msg = traceback.format_exc()
            self.logger.error(msg)
            
            return json.dumps(None)


    @cherrypy.expose     
    def getCompanyList(self, sign = None, lang = 'zh'):
        try:
            self.logger.info("get request %s %s" % (str(sign), lang))
                      
            data = self.data_source.getCompanyList(sign, lang)
            
            return json.dumps(data)
        except:
            msg = traceback.format_exc()
            self.logger.error(msg)
            
            return json.dumps(None)
        
        
    @cherrypy.expose     
    def getAirportList(self, sign = None, lang = 'zh'):
        try:
            self.logger.info("get request %s %s" % (str(sign), lang))
                      
            data = self.data_source.getAirportList(sign, lang)
            
            return json.dumps(data)
        except:
            msg = traceback.format_exc()
            self.logger.error(msg)
            
            return json.dumps(None)
        
    
    @cherrypy.expose     
    def getAirportWeather(self, airport, wtype = 'realtime', lang = 'zh'):
        try:
            self.logger.info("get request %s %s %s" % (airport, wtype, lang))
                      
            data = self.data_source.getAirportWeather(airport, wtype, lang)
            
            return json.dumps(data)
        except:
            msg = traceback.format_exc()
            self.logger.error(msg)
            
            return json.dumps(None)
        
        
    @cherrypy.expose     
    def getPushInfo(self, device_token = "", push_switch = ""):
        try:
            self.logger.info("get request %s %s" % (device_token, push_switch))
            
            data = self.data_source.getPushInfoList(device_token, push_switch)
           
            template = Template(filename = 'templates/PushInfo.txt')
            if self.config.debug_mode == 'no':
                return template.render(rows = data, ip = "118.194.161.243", port = "28888")
            else:
                return template.render(rows = data, ip = self.config.http_ip, port = self.config.http_port)
        except:
            msg = traceback.format_exc()
            self.logger.error(msg)
            
            return json.dumps(None)
        
    
    @cherrypy.expose     
    def spiderFlightFixInfo(self):
        try:
            self.logger.info("get request")
                      
            self.data_source.spiderFlightFixInfo()
            
            return json.dumps('OK')
        except:
            msg = traceback.format_exc()
            self.logger.error(msg)
            
            return json.dumps(None)
          
    
    '''
    @cherrypy.expose     
    def spiderPunctuality(self):
        try:
            self.logger.info("get request")
                      
            data = self.data_source.spiderPunctuality()
            
            return json.dumps(data)
        except:
            msg = traceback.format_exc()
            self.logger.error(msg)
            
            return json.dumps(None)

    
    
    @cherrypy.expose     
    def spiderAirline(self):
        try:
            self.logger.info("get request")
                      
            data = self.data_source.spiderAirline()
            
            return json.dumps(data)
        except:
            msg = traceback.format_exc()
            self.logger.error(msg)
            
            return json.dumps(None)
    '''


def main():
    if len(sys.argv) < 2:
        print '\nUsage: ' + sys.argv[0] + ' fd.cfg\n'
        sys.exit(-1)
        
    config = Config.Config(sys.argv[1])
    
    global_cfg = {
        'global' : {
            'server.socket_host' : str(config.http_ip),
            'server.socket_port' : int(config.http_port),
            'server.thread_pool' : int(config.thread_pool),
            'server.request_queue_size' : int(config.request_queue_size),
            'server.socket_timeout': int(config.timeout),
            'request.show_tracebacks' : False,
            'response.timeout': int(config.timeout),
            'engine.autoreload_on' : False,
            'log.screen': config.log_output,
            'log.error_file': config.log_error_file,
            'log.access_file': config.log_access_file,
            'environment': config.environment,
            'tools.gzip.on': config.gzip
        }
    }
    
    current_dir = os.path.dirname(os.path.abspath(__file__))
    
    cfg = {
        '/css' : {
            'tools.staticdir.on' : True,
            'tools.staticdir.dir' : "css",
            'tools.staticdir.root' : current_dir
        }
    }
    
    cherrypy.config.update(global_cfg)
    cherrypy.quickstart(FlightDaemon(config), '/', config = cfg)
      

if __name__ == '__main__':
    main()
    
    


