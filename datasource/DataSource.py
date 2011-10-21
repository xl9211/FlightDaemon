# coding=utf-8

import urllib2
from tools import LogUtil
from site import Ctrip
from site import Qunar
from site import Veryzhun
from site import Other
from db import DBUtility
import traceback
import time
import json


class DataSource:
    
    def __init__(self, config):
        self.config = config
        self.logger = LogUtil.Logging.getLogger()

        DBUtility.init(self.config.db_user, self.config.db_passwd, self.config.db_host, self.config.db_name)
    
    
    def createDataSource(self, source):
        if source == 'ctrip':
            return Ctrip.Ctrip(self.config)
        if source == 'veryzhun':
            return Veryzhun.Veryzhun(self.config)
        if source == 'db':
            return DBUtility.DB()
        if source == 'other':
            return Other.Other(self.config)
        if source == 'qunar':
            return Qunar.Qunar(self.config)
        
        
    def getFlightFixInfoByFlightNO(self, flight_no, schedule_takeoff_date):
        try:
            self.logger.info("%s %s" % (flight_no, schedule_takeoff_date))
           
            db_data_source = self.createDataSource('db')
            data = db_data_source.getFlightFixInfoByFlightNO(flight_no, schedule_takeoff_date)
            
            return data
        except:
            msg = traceback.format_exc()
            self.logger.error(msg)
            
            return None
    

    def getFlightFixInfoByRoute(self, takeoff_airport, arrival_airport, schedule_takeoff_date, company):
        try:
            self.logger.info("%s %s %s %s" % (takeoff_airport, arrival_airport, schedule_takeoff_date, company))
            
            db_data_source = self.createDataSource('db')
            data = db_data_source.getFlightFixInfoByRoute(takeoff_airport, arrival_airport, schedule_takeoff_date, company)
            
            return data
        except:
            msg = traceback.format_exc()
            self.logger.error(msg)
            
            return None
        
        
    def getFlightFixInfoByUniq(self, flight_no, takeoff_city, arrival_city, schedule_takeoff_date):
        try:
            self.logger.info("%s %s %s %s %s" % (flight_no, takeoff_city, arrival_city, schedule_takeoff_date, 'ctrip'))
           
            db_data_source = self.createDataSource('db')
            data = db_data_source.getFlightFixInfoByUniq(flight_no, takeoff_city, arrival_city, schedule_takeoff_date)
            
            return data
        except:
            msg = traceback.format_exc()
            self.logger.error(msg)
            
            return None
        

    def getFlightRealtimeInfo(self, flight_no, schedule_takeoff_time, schedule_arrival_time, schedule_takeoff_date):
        try:
            self.logger.info("%s %s %s %s" % (flight_no, schedule_takeoff_time, schedule_arrival_time, schedule_takeoff_date))
            
            db_data_source = self.createDataSource('db')
            data = db_data_source.getFlightRealtimeInfo(flight_no, schedule_takeoff_time, schedule_arrival_time, schedule_takeoff_date)
            
            if data['full_info'] != 1:
                if data['full_info'] == -1:
                    data_list = []
                    data_list.append(data)
                    db_data_source.putFlightRealtimeInfo(data_list)

                cur_date = time.strftime("%Y-%m-%d", time.localtime())
                if cur_date == schedule_takeoff_date:
                    data_source = self.createDataSource('veryzhun')          
                    data = data_source.getFlightRealTimeInfo(flight_no, schedule_takeoff_date)

                    if data is None or len(data) == 0:
                        self.logger.error("get %s realtime info error" % (flight_no))
                    else:
                        db_data_source.putFlightRealtimeInfo(data)
                        
                        if len(data) == 1:
                            return data[0]
                        
                        for one in data:
                            if one['schedule_takeoff_time'] == schedule_takeoff_time and one['schedule_arrival_time'] == schedule_arrival_time:
                                return one

            return data
        except:
            msg = traceback.format_exc()
            self.logger.error(msg)
            
            return None
        
    
    def completeFlightInfo(self, data_list, fix_data_list, schedule_takeoff_date, lang, realtime_data_only = False):
        try:
            for fix_data in fix_data_list:
                realtime_data = self.getFlightRealtimeInfo(fix_data['flight_no'], fix_data['schedule_takeoff_time'], fix_data['schedule_arrival_time'], schedule_takeoff_date)

                data = {}
                
                if not realtime_data_only: 
                    data['flight_no'] = fix_data['flight_no']
                    data['company'] = self.getCompanyName(fix_data['company'], lang)
                    data['takeoff_airport_entrance_exit'] = ""
                    data['takeoff_airport'] = self.getAirportName(fix_data['takeoff_airport'], lang)
                    data['takeoff_city'] = fix_data['takeoff_city']
                    data['arrival_airport_entrance_exit'] = ""
                    data['arrival_airport'] = self.getAirportName(fix_data['arrival_airport'], lang)
                    data['arrival_city'] = fix_data['arrival_city']
                    data['mileage'] = fix_data['mileage']
                    data['schedule_takeoff_date'] = schedule_takeoff_date
                    
                    if realtime_data['plane_model'] == "":
                        data['plane_model'] = fix_data['plane_model']
                    else:
                        data['plane_model'] = realtime_data['plane_model']
                    if realtime_data['takeoff_airport_building'] == "":
                        data['takeoff_airport_building'] = fix_data['takeoff_airport_building']
                    else:  
                        data['takeoff_airport_building'] = realtime_data['takeoff_airport_building']
                    if realtime_data['arrival_airport_building']:
                        data['arrival_airport_building'] = fix_data['arrival_airport_building']
                    else:
                        data['arrival_airport_building'] = realtime_data['arrival_airport_building']
                
                data['schedule_takeoff_time'] = fix_data['schedule_takeoff_time']
                data['schedule_arrival_time'] = fix_data['schedule_arrival_time']
                data['flight_state'] = realtime_data['flight_state'] 
                data['estimate_takeoff_time'] = realtime_data['estimate_takeoff_time']
                data['actual_takeoff_time'] = realtime_data['actual_takeoff_time']        
                data['estimate_arrival_time'] = realtime_data['estimate_arrival_time']
                data['actual_arrival_time'] = realtime_data['actual_arrival_time']
                data['flight_location'] = realtime_data['flight_location']
                
                data_list.append(data)
            
            return 0
        except:
            msg = traceback.format_exc()
            self.logger.error(msg)
            
            return json.dumps(None)
        
        
    def getRandomFlight(self):
        try:
            db_data_source = self.createDataSource('db')
            flight_no = db_data_source.getRandomFlight()
            
            return flight_no
        except:
            msg = traceback.format_exc()
            self.logger.error(msg)
            
            return json.dumps(None)
    
    
    def getCompanyList(self, lang):
        try:
            db_data_source = self.createDataSource('db')
            data = db_data_source.getCompanyList(lang)
            
            return data
        except:
            msg = traceback.format_exc()
            self.logger.error(msg)
            
            return json.dumps(None)
        
    
    def getAirportList(self, lang):
        try:
            db_data_source = self.createDataSource('db')
            data = db_data_source.getAirportList(lang)
            
            return data
        except:
            msg = traceback.format_exc()
            self.logger.error(msg)
            
            return json.dumps(None)
        
    
    def getCompanyName(self, short, lang):
        try:
            db_data_source = self.createDataSource('db')
            data = db_data_source.getCompanyName(short, lang)
            
            return data
        except:
            msg = traceback.format_exc()
            self.logger.error(msg)
            
            return json.dumps(None)
        
    
    def getAirportName(self, short, lang):
        try:
            db_data_source = self.createDataSource('db')
            data = db_data_source.getAirportName(short, lang)
            
            return data
        except:
            msg = traceback.format_exc()
            self.logger.error(msg)
            
            return json.dumps(None)
    

    #########################################################################################
    # 一次性使用
    '''
    def spiderCompany(self):
        try:
            data = []
            data_source = self.createDataSource('other')          
            data = data_source.getCompany()
            
            db_data_source = self.createDataSource('db')
            db_data_source.putCompany(data)
        except:
            msg = traceback.format_exc()
            self.logger.error(msg)
            
            return json.dumps(None)
        
    
    def spiderAirline(self):
        try:
            db_data_source = self.createDataSource('db')
            city_list = db_data_source.getCityList('zh')

            data_source = self.createDataSource('qunar')
            for city in city_list:
                data = data_source.getAirline(city)
                db_data_source.putAirline(data)
                self.logger.info(city)
        except:
            msg = traceback.format_exc()
            self.logger.error(msg)
            
            return json.dumps(None)
        
    
    def spiderFlightFixInfo(self):
        try:
            db_data_source = self.createDataSource('db')
            airline_list = db_data_source.getAirlineList()

            data_source = self.createDataSource('qunar')
            for airline in airline_list:
                self.logger.info("%s %s" % (airline['takeoff_city'], airline['arrival_city']))
                data = data_source.getFlightFixInfoByAirline(airline['takeoff_city'], airline['arrival_city'])
                db_data_source.putFlightFixInfo(data)
                
                db_data_source.putAirportInfo(data)  
        except:
            msg = traceback.format_exc()
            self.logger.error(msg)
            
            return json.dumps(None)
    '''
    # 一次性使用
    #########################################################################################    
        
    
    
    
    
    
    
    
