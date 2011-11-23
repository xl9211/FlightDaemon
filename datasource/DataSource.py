# coding=utf-8


from tools import LogUtil
from site import Ctrip
from site import Qunar
from site import Veryzhun
from site import Other
from site import Feeyo
from site import Weather
from db import DBUtility
import traceback
import time
import json
from db import DBBase #@UnusedImport
import hashlib


class DataSource:
    
    def __init__(self, config):
        self.config = config
        self.logger = LogUtil.Logging.getLogger()

        DBUtility.init(self.config.db_user, self.config.db_passwd, self.config.db_host, self.config.db_name)
        
        self.db_data_source = self.createDataSource('db')
        self.qunar_source = self.createDataSource('qunar')
        self.punctuality_source = self.createDataSource('feeyo')
        self.weather_source = self.createDataSource('weather')
    
    
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
        if source == 'weather':
            return Weather.Weather(self.config)
        
        
    def getFlightFixInfoByFlightNO(self, flight_no, schedule_takeoff_date):
        try:
            self.logger.info("%s %s" % (flight_no, schedule_takeoff_date))
                     
            flight_list = self.db_data_source.getFlightFixInfoByFlightNO(flight_no)
            flight_list = self.__checkFixDataValid(flight_list, schedule_takeoff_date)
            
            return flight_list
        except:
            msg = traceback.format_exc()
            self.logger.error(msg)
            
            return None
    

    def getFlightFixInfoByRoute(self, takeoff_airport, arrival_airport, schedule_takeoff_date, company):
        try:
            self.logger.info("%s %s %s %s" % (takeoff_airport, arrival_airport, schedule_takeoff_date, company))
            
            flight_list = self.db_data_source.getFlightFixInfoByRoute(takeoff_airport, arrival_airport, company)
            flight_list = self.__checkFixDataValid(flight_list, schedule_takeoff_date)
            
            return flight_list
        except:
            msg = traceback.format_exc()
            self.logger.error(msg)
            
            return None
        
        
    def getFlightFixInfoByUniq(self, flight_no, takeoff_airport, arrival_airport, schedule_takeoff_date):
        try:
            self.logger.info("%s %s %s %s" % (flight_no, takeoff_airport, arrival_airport, schedule_takeoff_date))
                     
            flight_list = self.db_data_source.getFlightFixInfoByUniq(flight_no, takeoff_airport, arrival_airport)
            flight_list = self.__checkFixDataValid(flight_list, schedule_takeoff_date)
            
            return flight_list
        except:
            msg = traceback.format_exc()
            self.logger.error(msg)
            
            return None
        
        
    def __checkFixDataValid(self, flight_list, schedule_takeoff_date):
        try:
            week = time.strftime("%w", time.strptime(schedule_takeoff_date, "%Y-%m-%d"))
            if week == '0':
                week = '7'

            filter_dup = []
            flight_list_ret = []
            for flight in flight_list:
                if week in flight['schedule']:
                    key = "%s%s%s" % (flight['flight_no'], flight['takeoff_airport'], flight['arrival_airport'])
                    if key not in filter_dup:
                        filter_dup.append(key)
                        flight_list_ret.append(flight)
            
            return flight_list_ret
        except:
            msg = traceback.format_exc()
            self.logger.error(msg)
            
            return None
        
    
    def getFlightRealtimeInfo(self, flight):
        try:
            self.logger.info("%s %s %s %s" % (flight['flight_no'],
                                              flight['takeoff_airport'],
                                              flight['arrival_airport'], 
                                              flight['schedule_takeoff_date']))      
        
            self.db_data_source.getFlightRealtimeInfo(flight)
            
            flight_state = flight['flight_state']
            
            self.spiderFlightRealtimeInfo(flight, False)
            
            self.db_data_source.putFlightRealtimeInfo(flight)
            
            if flight_state != flight['flight_state']:
                # 状态发生变化
                return 1

            return 0
        except:
            msg = traceback.format_exc()
            self.logger.error(msg)
            
            return None
        
        
    def spiderFlightRealtimeInfo(self, flight, auto):
        try:
            if flight['full_info'] == 0 and self.allow2Spider(flight, auto):      
                ret = self.qunar_source.getFlightRealTimeInfo(flight)
    
                if ret is None:
                    self.logger.error("get %s realtime info error" % (flight['flight_no']))
                else:
                    self.logger.info("get %s realtime info succ" % (flight['flight_no']))
        except:
            msg = traceback.format_exc()
            self.logger.error(msg)
            
            return None
        
        
    def allow2Spider(self, flight, auto):
        cur_date = time.strftime("%Y-%m-%d", time.localtime())
        
        cur_time = time.strftime("%H:%M", time.localtime())
        cur_hour = int(cur_time[:2])
        cur_minute = int(cur_time[3:]) + cur_hour * 60
        
        hour = int(flight['schedule_takeoff_time'][:2])
        minute = int(flight['schedule_takeoff_time'][3:]) + hour * 60
        
        if auto:
            if cur_date < flight['schedule_takeoff_date']:
                self.logger.info("%s %s not allow to spider" %(flight['flight_no'], flight['schedule_takeoff_date']))
                return False
            elif cur_date > flight['schedule_takeoff_date']:
                cur_minute += 60 * 24

            if cur_minute > minute:
                self.logger.info("%s %s allow to spider" %(flight['flight_no'], flight['schedule_takeoff_date']))
                return True
            else:
                self.logger.info("%s %s not allow to spider" %(flight['flight_no'], flight['schedule_takeoff_date']))
                return False
        else:
            if cur_date == flight['schedule_takeoff_date'] and (minute - cur_minute) < 60 * 3:
                self.logger.info("%s %s allow to spider" %(flight['flight_no'], flight['schedule_takeoff_date']))
                return True
            else:
                self.logger.info("%s %s not allow to spider" %(flight['flight_no'], flight['schedule_takeoff_date']))
                return False
                

    def completeFlightInfo(self, flight_list, schedule_takeoff_date, lang):
        try:
            for flight in flight_list:
                flight['schedule_takeoff_date'] = schedule_takeoff_date
                flight['flight_state'] = u"计划航班"
                flight['estimate_takeoff_time'] = "--:--"
                flight['actual_takeoff_time'] = "--:--"
                flight['estimate_arrival_time'] = "--:--"
                flight['actual_arrival_time'] = "--:--"
                flight['full_info'] = 0
                flight['flight_location'] = ""
                
                self.getFlightRealtimeInfo(flight)
                 
                flight['company'] = self.getCompanyName(flight['company'], lang)
                flight['takeoff_airport'] = self.getAirportName(flight['takeoff_airport'], lang)
                flight['takeoff_city'] = self.getCityName(flight['takeoff_city'],lang)
                flight['arrival_airport'] = self.getAirportName(flight['arrival_airport'], lang)
                flight['arrival_city'] = self.getCityName(flight['arrival_city'], lang)
                
                flight['takeoff_airport_entrance_exit'] = ""
                flight['arrival_airport_entrance_exit'] = "" 
                        
                flight['on_time'] = ""
                flight['half_hour_late'] = ""
                flight['one_hour_late'] = ""
                flight['more_than_one_hour_late'] = ""
                flight['cancel'] = ""
                
                '''
                punctuality_data = self.getPunctualityInfo(fix_data, spider_punctuality)
                if punctuality_data is not None:
                    data['on_time'] = punctuality_data['on_time']
                    data['half_hour_late'] = punctuality_data['half_hour_late']
                    data['one_hour_late'] = punctuality_data['one_hour_late']
                    data['more_than_one_hour_late'] = punctuality_data['more_than_one_hour_late']
                    data['cancel'] = punctuality_data['cancel']
                '''
                
                if flight['estimate_takeoff_time'] == flight['schedule_takeoff_time']:
                    flight['estimate_takeoff_time'] = "--:--"
         
                if flight['estimate_arrival_time'] == flight['schedule_arrival_time']:
                    flight['estimate_arrival_time'] = "--:--"
                
                self.checkRealtimeDataValid(flight)
            return 0
        except:
            msg = traceback.format_exc()
            self.logger.error(msg)
            
            return None
        
        
    def checkRealtimeDataValid(self, flight):
        try:
            flight['valid'] = '1'
            cur_time = time.strftime("%H:%M", time.localtime())
            
            if flight['flight_state'] == u"已经起飞" and (flight['actual_takeoff_time'] == '--:--' or flight['actual_takeoff_time'] > cur_time):
                flight['valid'] = '0'

            if flight['flight_state'] == u"已经到达" and (flight['actual_arrival_time'] == '--:--' or flight['actual_arrival_time'] > cur_time):
                flight['valid'] = '0'
            
            return 0
        except:
            msg = traceback.format_exc()
            self.logger.error(msg)
            
            return None

        
    def getRandomFlightList(self, cur_time):
        try:
            flight_list = self.db_data_source.getRandomFlightList(cur_time)
            
            return flight_list
        except:
            msg = traceback.format_exc()
            self.logger.error(msg)
            
            return None
        
    
    def getRandomFlight(self):
        try:
            flight = self.db_data_source.getRandomFlight()
            
            return flight
        except:
            msg = traceback.format_exc()
            self.logger.error(msg)
            
            return None
        
    
    def getPushCandidate(self, flight):
        try:
            push_list = self.db_data_source.getPushCandidate(flight)
            
            return push_list
        except:
            msg = traceback.format_exc()
            self.logger.error(msg)
            
            return None
    
    
    def getCompanyList(self, sign, lang):
        try:
            data = self.db_data_source.getCompanyList(lang)
            if sign is None:
                return data
            else:
                hash = {} #@ReservedAssignment
                m = hashlib.md5()
                m.update(json.dumps(data))
                sign_new = m.hexdigest().upper()
                if sign_new != sign:
                    hash['sign'] = sign_new
                    hash['data'] = data
                return hash
        except:
            msg = traceback.format_exc()
            self.logger.error(msg)
            
            return None
        
    
    def getAirportList(self, sign, lang):
        try:
            data = self.db_data_source.getAirportList(lang)
            if sign is None:
                return data
            else:
                hash = {} #@ReservedAssignment
                m = hashlib.md5()
                m.update(json.dumps(data))
                sign_new = m.hexdigest().upper()
                if sign_new != sign:
                    hash['sign'] = sign_new
                    hash['data'] = data
                return hash
        except:
            msg = traceback.format_exc()
            self.logger.error(msg)
            
            return None
        
    
    def getCompanyName(self, short, lang):
        try:
            data = self.db_data_source.getCompanyName(short, lang)
            
            return data
        except:
            msg = traceback.format_exc()
            self.logger.error(msg)
            
            return None
        
    
    def getCityName(self, short, lang):
        try:
            data = self.db_data_source.getCityName(short, lang)
            
            return data
        except:
            msg = traceback.format_exc()
            self.logger.error(msg)
            
            return None
        
    
    def getAirportName(self, short, lang):
        try:
            data = self.db_data_source.getAirportName(short, lang)
            
            return data
        except:
            msg = traceback.format_exc()
            self.logger.error(msg)
            
            return None
        
    
    def getAllLivedFlight(self):
        try:
            data = self.db_data_source.getAllLivedFlight()
            
            return data
        except:
            msg = traceback.format_exc()
            self.logger.error(msg)
            
            return None
        
    
    def getAirportWeather(self, airport, wtype = 'realtime', lang = 'zh'):
        try:
            weather_info = {}
            
            city = self.db_data_source.getAirportCity(airport)
            if city is not None:
                city_code = self.db_data_source.getCityCode(city)
                if city_code is not None:
                    weather_info = self.weather_source.getWeather(city_code, wtype)
                    weather_info['airport'] = self.db_data_source.getAirportName(airport, lang)

            return weather_info
        except:
            msg = traceback.format_exc()
            self.logger.error(msg)
            
            return None
        
        
    def getPushInfoList(self, device_token, push_switch):
        try:
            push_list = self.db_data_source.getPushInfoList(device_token, push_switch)
            return push_list
        except:
            msg = traceback.format_exc()
            self.logger.error(msg)
            
            return None
        
        
    def storeFollowedInfo(self, device_token, followed_list):
        try:
            self.db_data_source.putFollowedInfo(device_token, followed_list)
        except:
            msg = traceback.format_exc()
            self.logger.error(msg)
        
            return None
        
    
    def deleteFollowedInfo(self, device_token, followed_list):
        try:
            self.db_data_source.deleteFollowedInfo(device_token, followed_list)
        except:
            msg = traceback.format_exc()
            self.logger.error(msg)
        
            return None

        
    def storePushInfo(self, push_candidate, flight):
        try:
            self.db_data_source.putPushInfo(push_candidate, flight)
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
            
            return None
    
    
    def spiderFlightFixInfo(self):
        try:
            db_data_source = self.createDataSource('db')
            airline_list = db_data_source.getAirlineList()

            data_source = self.createDataSource('qunar')
            all_num = len(airline_list)
            count = 0
            for airline in airline_list:
                count += 1
                self.logger.info("%s/%s, %s %s" % (str(count), str(all_num), airline['takeoff_city'], airline['arrival_city']))
                data = data_source.getFlightFixInfoByAirline(airline['takeoff_city'], airline['arrival_city'])
                db_data_source.putFlightFixInfo(data)
                
                db_data_source.putAirportInfo(data)  
        except:
            msg = traceback.format_exc()
            self.logger.error(msg)
            
            return None
    
    
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
            
            return None
        
    
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
            
            return None
    '''


    # 一次性使用
    #########################################################################################    
        
    
    
    
    
    
    
    
