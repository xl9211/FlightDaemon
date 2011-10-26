# coding=utf-8

import urllib2
from tools import LogUtil
from site import Ctrip
from site import Qunar
from site import Veryzhun
from site import Other
from site import Feeyo
from db import DBUtility
import traceback
import time
import json


class DataSource:
    
    def __init__(self, config):
        self.config = config
        self.logger = LogUtil.Logging.getLogger()

        DBUtility.init(self.config.db_user, self.config.db_passwd, self.config.db_host, self.config.db_name)
        
        self.db_data_source = self.createDataSource('db')
        self.realtime_data_source = []
        self.realtime_data_source.append(self.createDataSource('qunar'))
        self.punctuality_source = self.createDataSource('feeyo')
    
    
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
        if source == 'feeyo':
            return Feeyo.Feeyo(self.config)
        
        
    def getFlightFixInfoByFlightNO(self, flight_no, schedule_takeoff_date):
        try:
            self.logger.info("%s %s" % (flight_no, schedule_takeoff_date))
                       
            data = self.db_data_source.getFlightFixInfoByFlightNO(flight_no, schedule_takeoff_date)
            
            return data
        except:
            msg = traceback.format_exc()
            self.logger.error(msg)
            
            return None
    

    def getFlightFixInfoByRoute(self, takeoff_airport, arrival_airport, schedule_takeoff_date, company):
        try:
            self.logger.info("%s %s %s %s" % (takeoff_airport, arrival_airport, schedule_takeoff_date, company))
            
            data = self.db_data_source.getFlightFixInfoByRoute(takeoff_airport, arrival_airport, schedule_takeoff_date, company)
            
            return data
        except:
            msg = traceback.format_exc()
            self.logger.error(msg)
            
            return None
        
        
    def getFlightFixInfoByUniq(self, flight_no, takeoff_city, arrival_city, schedule_takeoff_date, lang):
        try:
            self.logger.info("%s %s %s %s" % (flight_no, takeoff_city, arrival_city, schedule_takeoff_date))

            data = self.db_data_source.getFlightFixInfoByUniq(flight_no, takeoff_city, arrival_city, schedule_takeoff_date, lang)
            
            return data
        except:
            msg = traceback.format_exc()
            self.logger.error(msg)
            
            return None
        
    
    def getPunctualityInfo(self, fix_data, spider_punctuality):
        try:
            self.logger.info("%s %s %s" % (fix_data['flight_no'], fix_data['takeoff_airport'], fix_data['arrival_airport']))

            data = self.db_data_source.getPunctualityInfo(fix_data['flight_no'], fix_data['takeoff_airport'], fix_data['arrival_airport'])

            if data is None and spider_punctuality:
                data = self.punctuality_source.getPunctualityInfo(fix_data['takeoff_airport'], fix_data['arrival_airport'], fix_data['flight_no'])
                if data is not None:
                    self.db_data_source.putPunctualityInfo(fix_data, data)   
            
            return data
        except:
            msg = traceback.format_exc()
            self.logger.error(msg)
            
            return None
        

    def getFlightRealtimeInfo(self, fix_data, auto = False):
        try:
            self.logger.info("%s %s" % (fix_data['flight_no'], fix_data['schedule_takeoff_date']))
            
            flight = {}
            
            flight['flight_no'] = fix_data['flight_no']
            flight['schedule_takeoff_time'] = fix_data['schedule_takeoff_time']
            flight['schedule_arrival_time'] = fix_data['schedule_arrival_time']
            flight['takeoff_airport'] = fix_data['takeoff_airport']
            flight['arrival_airport'] = fix_data['arrival_airport']
            flight['schedule_takeoff_date'] = fix_data['schedule_takeoff_date']
             
            flight['flight_state'] = u"计划航班"
            flight['estimate_takeoff_time'] = "--:--"
            flight['actual_takeoff_time'] = "--:--"
            flight['estimate_arrival_time'] = "--:--"
            flight['actual_arrival_time'] = "--:--"
            flight['full_info'] = 0
            
            flight['flight_location'] = ""
        
            self.db_data_source.getFlightRealtimeInfo(flight)
            
            if flight['full_info'] == 0 and (not auto or self.allow2Spider(flight)): 
                for source in self.realtime_data_source:     
                    ret = source.getFlightRealTimeInfo(flight)
    
                    if ret is None:
                        self.logger.error("get %s realtime info error" % (flight['flight_no']))
                    elif ret == -1:
                        self.logger.info("today %s there is no such flight" % (flight['schedule_takeoff_date']))
                        return -1
                    else:
                        self.db_data_source.putFlightRealtimeInfo(flight)
                        break

            return flight
        except:
            msg = traceback.format_exc()
            self.logger.error(msg)
            
            return None
        
        
    def allow2Spider(self, flight):
        cur_date = time.strftime("%Y-%m-%d", time.localtime())
        
        cur_time = time.strftime("%H:%M", time.localtime())
        cur_hour = int(cur_time[:2])
        cur_minute = int(cur_time[3:])
        cur_second = cur_hour * 60 * 60 + cur_minute * 60
        
        if cur_date < flight['schedule_takeoff_date']:
            self.logger.info("%s %s not allow to spider" %(flight['flight_no'], flight['schedule_takeoff_date']))
            return False
        elif cur_date > flight['schedule_takeoff_date']:
            cur_second += 60 * 60 * 24
        
        
        hour = int(flight['schedule_arrival_time'][:2])
        minute = int(flight['schedule_arrival_time'][3:])
        second = hour * 60 * 60 + minute * 60
        
        if cur_second > second:
            self.logger.info("%s %s allow to spider" %(flight['flight_no'], flight['schedule_takeoff_date']))
            return True
        else:
            self.logger.info("%s %s not allow to spider" %(flight['flight_no'], flight['schedule_takeoff_date']))
            return False
        
    
    def completeFlightInfo(self, data_list, fix_data_list, schedule_takeoff_date, lang, realtime_data_only, spider_punctuality):
        try:
            for fix_data in fix_data_list:
                fix_data['schedule_takeoff_date'] = schedule_takeoff_date
                realtime_data = self.getFlightRealtimeInfo(fix_data)
                
                if realtime_data == -1:
                    continue

                data = {}
                
                if not realtime_data_only: 
                    data['flight_no'] = fix_data['flight_no']
                    data['company'] = self.getCompanyName(fix_data['company'], lang)
                    data['takeoff_airport_entrance_exit'] = ""
                    data['takeoff_airport'] = self.getAirportName(fix_data['takeoff_airport'], lang)
                    data['takeoff_city'] = self.getCityName(fix_data['takeoff_city'],lang)
                    data['arrival_airport_entrance_exit'] = ""
                    data['arrival_airport'] = self.getAirportName(fix_data['arrival_airport'], lang)
                    data['arrival_city'] = self.getCityName(fix_data['arrival_city'], lang)
                    data['mileage'] = fix_data['mileage']
                    data['schedule_takeoff_date'] = fix_data['schedule_takeoff_date']
                    data['plane_model'] = fix_data['plane_model']
                    data['takeoff_airport_building'] = fix_data['takeoff_airport_building']
                    data['arrival_airport_building'] = fix_data['arrival_airport_building']
                    data['on_time'] = ""
                    data['half_hour_late'] = ""
                    data['one_hour_late'] = ""
                    data['more_than_one_hour_late'] = ""
                    data['cancel'] = ""
                    
                    punctuality_data = self.getPunctualityInfo(fix_data, spider_punctuality)
                    if punctuality_data is not None:
                        data['on_time'] = punctuality_data['on_time']
                        data['half_hour_late'] = punctuality_data['half_hour_late']
                        data['one_hour_late'] = punctuality_data['one_hour_late']
                        data['more_than_one_hour_late'] = punctuality_data['more_than_one_hour_late']
                        data['cancel'] = punctuality_data['cancel']

                data['schedule_takeoff_time'] = realtime_data['schedule_takeoff_time']
                data['schedule_arrival_time'] = realtime_data['schedule_arrival_time']
                
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
            flight_no = self.db_data_source.getRandomFlight()
            
            return flight_no
        except:
            msg = traceback.format_exc()
            self.logger.error(msg)
            
            return json.dumps(None)
    
    
    def getCompanyList(self, lang):
        try:
            data = self.db_data_source.getCompanyList(lang)
            
            return data
        except:
            msg = traceback.format_exc()
            self.logger.error(msg)
            
            return json.dumps(None)
        
    
    def getAirportList(self, lang):
        try:
            data = self.db_data_source.getAirportList(lang)
            
            return data
        except:
            msg = traceback.format_exc()
            self.logger.error(msg)
            
            return json.dumps(None)
        
    
    def getCompanyName(self, short, lang):
        try:
            data = self.db_data_source.getCompanyName(short, lang)
            
            return data
        except:
            msg = traceback.format_exc()
            self.logger.error(msg)
            
            return json.dumps(None)
        
    
    def getCityName(self, short, lang):
        try:
            data = self.db_data_source.getCityName(short, lang)
            
            return data
        except:
            msg = traceback.format_exc()
            self.logger.error(msg)
            
            return json.dumps(None)
        
    
    def getAirportName(self, short, lang):
        try:
            data = self.db_data_source.getAirportName(short, lang)
            
            return data
        except:
            msg = traceback.format_exc()
            self.logger.error(msg)
            
            return json.dumps(None)
        
    
    def getAllLivedFlight(self):
        try:
            data = self.db_data_source.getAllLivedFlight()
            
            return data
        except:
            msg = traceback.format_exc()
            self.logger.error(msg)
            
            return json.dumps(None)
    

    #########################################################################################
    # 一次性使用
    
    def spiderPunctuality(self):
        try:
            db_data_source = self.createDataSource('db')
            flight_list = db_data_source.getFlightList()

            count = 0
            data_source = self.createDataSource('feeyo')
            for flight in flight_list:
                count += 1
                self.logger.info(str(count))
                ret = db_data_source.getPunctualityInfo(flight['flight_no'], flight['takeoff_airport'], flight['arrival_airport'])
                if ret is None:
                    punctualit_info = data_source.getPunctualityInfo(flight['takeoff_airport'], flight['arrival_airport'], flight['flight_no'])
                    db_data_source.putPunctualityInfo(flight, punctualit_info)
                    time.sleep(2)
                else:
                    self.logger.info("already in..................................................")    
        except:
            msg = traceback.format_exc()
            self.logger.error(msg)
            
            return json.dumps(None)
    
    
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
        
    
    
    
    
    
    
    
