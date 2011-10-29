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
import ConfigParser
import json
import time
from datasource.DataSource import DataSource
from task.FlightScan import FlightScan
import random


from pygments import highlight
from pygments.lexers import PythonLexer
from pygments.formatters import HtmlFormatter


class FlightDaemon:
    
    def __init__(self, config):
        self.config = config
        self.logger = LogUtil.Logging.getLogger()    
           
        self.data_source = DataSource(self.config)

        self.flight_scan = FlightScan(self.config, self.data_source)
        if self.config.debug_mode == 'no':
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
    def queryFlightInfoByRoute(self, takeoff_airport, arrival_airport, schedule_takeoff_date, company = 'all', lang = 'zh'):
        try:
            self.logger.info("get request %s %s %s %s %s" % (takeoff_airport, arrival_airport, schedule_takeoff_date, company, lang))
            
            fix_data_list = self.data_source.getFlightFixInfoByRoute(takeoff_airport, arrival_airport, schedule_takeoff_date, company)
            self.logger.info("fix data %s" %(json.dumps(fix_data_list)))
 
            if len(fix_data_list) == 0:
                return json.dumps([])
            if fix_data_list == None:
                return json.dumps(None)
            
            data_list = []
            self.data_source.completeFlightInfo(data_list, fix_data_list, schedule_takeoff_date, lang, False, False, False)
  
            ret = json.dumps(data_list)
            self.logger.info(ret) 
            return ret
        
        except:
            msg = traceback.format_exc()
            self.logger.error(msg)
            
            return json.dumps(None)
    
    
    @cherrypy.expose
    def queryFlightInfoByFlightNO(self, flight_no, schedule_takeoff_date, lang = 'zh', flying = False):
        try:
            self.logger.info("get request %s %s %s" % (flight_no, schedule_takeoff_date, lang))
            
            fix_data_list = self.data_source.getFlightFixInfoByFlightNO(flight_no, schedule_takeoff_date)
            self.logger.info("fix data %s" %(json.dumps(fix_data_list)))
 
            if len(fix_data_list) == 0:
                return json.dumps([])
            if fix_data_list == None:
                return json.dumps(None)
            
            data_list = []
            self.data_source.completeFlightInfo(data_list, fix_data_list, schedule_takeoff_date, lang, False, True, flying)
  
            ret = json.dumps(data_list)
            self.logger.info(ret) 
            return ret
        
        except:
            msg = traceback.format_exc()
            self.logger.error(msg)
            
            return json.dumps(None)
    
    
    @cherrypy.expose   
    def queryFlightInfoByRandom(self, lang = 'zh'):
        try:
            self.logger.info("get request %s" % (lang))
            
            flights = []
            cur_time = time.strftime("%H:%M", time.localtime())
            
            if not (cur_time > self.config.stop_fly_start and cur_time < self.config.stop_fly_end):
                flight_list = self.data_source.getRandomFlightList(cur_time)
                flight_num = len(flight_list)
                
                for i in range(0, flight_num):
                    index = random.randint(0, len(flight_list))
                    flight_no = flight_list.pop(index)
                    
                    schedule_takeoff_date = time.strftime("%Y-%m-%d", time.localtime())
                    flights = json.loads(self.queryFlightInfoByFlightNO(flight_no, schedule_takeoff_date, lang, True))
                    
                    if len(flights) > 0:
                        break
            
            if flights == []:
                flight = self.data_source.getRandomFlight()
                
                if flight is not None:
                    flights = json.loads(self.queryFlightInfoByFlightNO(flight['flight_no'], flight['schedule_takeoff_date'], lang, False))
            
            if flights is not None and len(flights) > 1:
                self.logger.info("random more than one")
                ret_tmp = []
                ret_tmp.append(flights[0])
                
                ret = json.dumps(ret_tmp)
                self.logger.info(ret)
                return ret
            
            ret = json.dumps(flights)
            self.logger.info(ret) 
            return ret              
        except:
            msg = traceback.format_exc()
            self.logger.error(msg)
            
            return json.dumps(None)

    
    @cherrypy.expose
    def updateFollowedFlightInfo(self, query_string, lang = 'zh'):
        try:
            self.logger.info("get request %s %s" % (query_string, lang))
            
            flight_list = json.loads(query_string)
            
            data_list = []
            for flight in flight_list:
                # fix data
                fix_data_list = self.data_source.getFlightFixInfoByUniq(flight['flight_no'], flight['takeoff_city'], flight['arrival_city'], flight['schedule_takeoff_date'], lang)
                self.logger.info("fix data %s" %(json.dumps(fix_data_list)))
 
                if len(fix_data_list) == 0:
                    self.logger.info("fix data not exist")
                    continue
                if fix_data_list == None:
                    self.logger.error("get fix data error")
                    continue
                
                self.data_source.completeFlightInfo(data_list, fix_data_list, flight['schedule_takeoff_date'], lang, True, False, False)
            
            ret = json.dumps(data_list)
            self.logger.info(ret) 
            return ret
        except:
            msg = traceback.format_exc()
            self.logger.error(msg)
            
            return json.dumps(None)
    
    
    @cherrypy.expose     
    def getCompanyList(self, sign = None, lang = 'zh'):
        try:
            self.logger.info("get request %s" % (lang))
                      
            data = self.data_source.getCompanyList(sign, lang)
            
            return json.dumps(data)
        except:
            msg = traceback.format_exc()
            self.logger.error(msg)
            
            return json.dumps(None)
        
        
    @cherrypy.expose     
    def getAirportList(self, sign = None, lang = 'zh'):
        try:
            self.logger.info("get request %s" % (lang))
                      
            data = self.data_source.getAirportList(sign, lang)
            
            return json.dumps(data)
        except:
            msg = traceback.format_exc()
            self.logger.error(msg)
            
            return json.dumps(None)
        
    
    @cherrypy.expose     
    def getAirportWeather(self, airport, type = 'realtime', lang = 'zh'):
        try:
            self.logger.info("get request %s" % (lang))
                      
            data = self.data_source.getAirportWeather(airport, type, lang)
            
            return json.dumps(data)
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

    
    @cherrypy.expose     
    def spiderFlightFixInfo(self):
        try:
            self.logger.info("get request")
                      
            data = self.data_source.spiderFlightFixInfo()
            
            return json.dumps('OK')
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
    
    cherrypy.quickstart(FlightDaemon(config), '/', config = global_cfg)
      

#Start AppManager
if __name__ == '__main__':
    main()
    
    


