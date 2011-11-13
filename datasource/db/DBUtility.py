# coding=utf-8

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm import scoped_session
from sqlalchemy.schema import Index
import DBBase

from FlightFixInfoModel import FlightFixInfo
from CompanyInfoModel import CompanyInfo
from CompanyInfoAllModel import CompanyInfoAll
from FlightRealtimeInfoModel import FlightRealtimeInfo
from AirportInfoModel import AirportInfo
from AirlineInfoModel import AirlineInfo
from CityInfoModel import CityInfo
from FollowedInfoModel import FollowedInfo
from PunctualityInfoModel import PunctualityInfo

import traceback
from tools import LogUtil
import json


def init(db_user, db_passwd, db_host, db_name):
        DBBase.Engine = create_engine("mysql://%s:%s@%s/%s?charset=utf8" %(db_user, db_passwd, db_host, db_name), pool_recycle = -1, echo = False)
        
        Index('ix_followedInfo_col23456',
              FollowedInfo.device_token,
              FollowedInfo.flight_no,
              FollowedInfo.takeoff_airport,
              FollowedInfo.arrival_airport,
              FollowedInfo.schedule_takeoff_date)
        
        DBBase.Base.metadata.create_all(DBBase.Engine)
        DBBase.Session = scoped_session(sessionmaker(bind = DBBase.Engine, expire_on_commit = False))


class DB:
    
    def __init__(self):
        self.logger = LogUtil.Logging.getLogger()
    
    
    def getFlightFixInfoByUniq(self, flight_no, takeoff_airport, arrival_airport, schedule_takeoff_date):
        try:
            ret = FlightFixInfo.find(flight_no = flight_no, takeoff_airport = takeoff_airport, arrival_airport = arrival_airport)
            flight_info_list = []
            for one in ret:
                one_hash = {}
                
                one_hash['flight_no'] = one.flight_no
                one_hash['company'] = one.company
                one_hash['schedule_takeoff_time'] = one.schedule_takeoff_time
                one_hash['schedule_arrival_time'] = one.schedule_arrival_time
                one_hash['takeoff_city'] = one.takeoff_city
                one_hash['takeoff_airport'] = one.takeoff_airport
                one_hash['takeoff_airport_building'] = one.takeoff_airport_building
                one_hash['arrival_city'] = one.arrival_city
                one_hash['arrival_airport'] = one.arrival_airport
                one_hash['arrival_airport_building'] = one.arrival_airport_building
                one_hash['plane_model'] = one.plane_model
                one_hash['mileage'] = one.mileage
                
                flight_info_list.append(one_hash)
                break
            
            return flight_info_list
        except:
            msg = traceback.format_exc()
            self.logger.error(msg)
            
            DBBase.Session.rollback()
            DBBase.Engine.dispose()
            
            return None
    
     
    def getFlightFixInfoByFlightNO(self, flight_no, schedule_takeoff_date):
        try:
            ret = FlightFixInfo.find(flight_no = flight_no)
            flight_info_list = []
            for one in ret:
                one_hash = {}
                
                one_hash['flight_no'] = one.flight_no
                one_hash['company'] = one.company
                one_hash['schedule_takeoff_time'] = one.schedule_takeoff_time
                one_hash['schedule_arrival_time'] = one.schedule_arrival_time
                one_hash['takeoff_city'] = one.takeoff_city
                one_hash['takeoff_airport'] = one.takeoff_airport
                one_hash['takeoff_airport_building'] = one.takeoff_airport_building
                one_hash['arrival_city'] = one.arrival_city
                one_hash['arrival_airport'] = one.arrival_airport
                one_hash['arrival_airport_building'] = one.arrival_airport_building
                one_hash['plane_model'] = one.plane_model
                one_hash['mileage'] = one.mileage
                
                flight_info_list.append(one_hash)
            
            return flight_info_list
        except:
            msg = traceback.format_exc()
            self.logger.error(msg)
            
            DBBase.Session.rollback()
            DBBase.Engine.dispose()
            
            return None
    
    
    def getFlightFixInfoByRoute(self, takeoff_airport, arrival_airport, schedule_takeoff_date, company):
        try:
            ret = []
            if company == 'all':
                ret = FlightFixInfo.find(takeoff_airport = takeoff_airport, arrival_airport = arrival_airport)
            else:
                ret = FlightFixInfo.find(takeoff_airport = takeoff_airport, arrival_airport = arrival_airport, company = company)
            
            flight_info_list = []
            for one in ret:
                one_hash = {}
                
                one_hash['flight_no'] = one.flight_no
                one_hash['company'] = one.company
                one_hash['schedule_takeoff_time'] = one.schedule_takeoff_time
                one_hash['schedule_arrival_time'] = one.schedule_arrival_time
                one_hash['takeoff_city'] = one.takeoff_city
                one_hash['takeoff_airport'] = one.takeoff_airport
                one_hash['takeoff_airport_building'] = one.takeoff_airport_building
                one_hash['arrival_city'] = one.arrival_city
                one_hash['arrival_airport'] = one.arrival_airport
                one_hash['arrival_airport_building'] = one.arrival_airport_building
                one_hash['plane_model'] = one.plane_model
                one_hash['mileage'] = one.mileage
                
                flight_info_list.append(one_hash)
            
            return flight_info_list
        except:
            msg = traceback.format_exc()
            self.logger.error(msg)
            
            DBBase.Session.rollback()
            DBBase.Engine.dispose()
            
            return None

            
    def getFlightRealtimeInfo(self, flight):
        try:
            ret = FlightRealtimeInfo.find(flight_no = flight['flight_no'], 
                                          takeoff_airport = flight['takeoff_airport'], 
                                          arrival_airport = flight['arrival_airport'], 
                                          schedule_takeoff_date = flight['schedule_takeoff_date'])
            
            if len(ret) >= 1:
                one = ret[0]
                
                flight['flight_state'] = one.flight_state
                flight['estimate_takeoff_time'] = one.estimate_takeoff_time
                flight['actual_takeoff_time'] = one.actual_takeoff_time
                flight['estimate_arrival_time'] = one.estimate_arrival_time
                flight['actual_arrival_time'] = one.actual_arrival_time
                flight['full_info'] = one.full_info
                
            '''
            else:
                one  = FlightRealtimeInfo()
                
                one.flight_no = flight['flight_no']
                one.flight_state = flight['flight_state']
                one.estimate_takeoff_time = flight['estimate_takeoff_time']
                one.actual_takeoff_time = flight['actual_takeoff_time'] 
                one.estimate_arrival_time = flight['estimate_arrival_time']
                one.actual_arrival_time = flight['actual_arrival_time']
                one.schedule_takeoff_time = flight['schedule_takeoff_time']
                one.schedule_arrival_time = flight['schedule_arrival_time']
                one.takeoff_airport = flight['takeoff_airport']
                one.arrival_airport = flight['arrival_airport']
                one.schedule_takeoff_date = flight['schedule_takeoff_date']
                one.full_info = 0    
                one.add()
            '''
        
            return flight
        except:
            msg = traceback.format_exc()
            self.logger.error(msg)
            
            DBBase.Session.rollback()
            DBBase.Engine.dispose()
            
            return None
    
    
    def getFlightList(self):
        try:
            ret = FlightFixInfo.find()
            
            flight_info_list = []
            for one in ret:
                one_hash = {}
                
                one_hash['flight_no'] = one.flight_no
                one_hash['takeoff_airport'] = one.takeoff_airport
                one_hash['arrival_airport'] = one.arrival_airport
                
                flight_info_list.append(one_hash)
            
            return flight_info_list
        except:
            msg = traceback.format_exc()
            self.logger.error(msg)
            
            DBBase.Session.rollback()
            DBBase.Engine.dispose()
            
            return None
            

    def getCompanyList(self, lang):
        try:
            ret = CompanyInfo.find()
            
            company_info_list = []
            for one in ret:
                one_hash = {}
                one_hash['short'] = one.company_short
                if lang == 'zh':
                    one_hash['full'] = one.company_zh
                
                company_info_list.append(one_hash)
            
            return company_info_list
        except:
            msg = traceback.format_exc()
            self.logger.error(msg)
            
            DBBase.Session.rollback()
            DBBase.Engine.dispose()
            
            return None 
    
    
    def getRandomFlightList(self, cur_time):
        try:
            ret = FlightFixInfo.getNowFlightNO(cur_time)
            
            flight_list = []
            for one in ret:
                flight_list.append(one[0])
            
            return flight_list
        except:
            msg = traceback.format_exc()
            self.logger.error(msg)
            
            DBBase.Session.rollback()
            DBBase.Engine.dispose()
            
            return None
        
        
    def getPushCandidate(self):
        try:
            ret = FollowedInfo().findAll(push_switch = 'on')
            
            push_list = []
            for one in ret:
                one_hash = {}
                one_hash['device_token'] = one.device_token
                one_hash['flight_no'] = one.flight_no
                one_hash['takeoff_airport'] = one.takeoff_airport
                one_hash['arrival_airport'] = one.arrival_airport
                one_hash['schedule_takeoff_date'] = one.schedule_takeoff_date
                one_hash['push_switch'] = one.push_switch
                one_hash['push_info'] = json.loads(one.push_info)
                
                push_list.append(one_hash) 
            
            return push_list
        except:
            msg = traceback.format_exc()
            self.logger.error(msg)
            
            DBBase.Session.rollback()
            DBBase.Engine.dispose()
            
            return None


    def getRandomFlight(self):
        try:
            ret = FlightRealtimeInfo.getOneArrivedFlightNO()
            
            flight = None
            
            if ret is not None:
                flight = {}
                flight['flight_no'] = ret.flight_no
                flight['schedule_takeoff_date'] = ret.schedule_takeoff_date
                
            return flight
        except:
            msg = traceback.format_exc()
            self.logger.error(msg)
            
            DBBase.Session.rollback()
            DBBase.Engine.dispose()
            
            return None
        
    
    def getAllLivedFlight(self):
        try:
            flights = FlightRealtimeInfo.find(full_info = 0)
            
            lived_flight_list = []
            for flight in flights:
                lived_flight = {}
                lived_flight['flight_no'] = flight.flight_no
                lived_flight['schedule_takeoff_time'] = flight.schedule_takeoff_time
                lived_flight['schedule_arrival_time'] = flight.schedule_arrival_time
                lived_flight['takeoff_airport'] = flight.takeoff_airport
                lived_flight['arrival_airport'] = flight.arrival_airport
                lived_flight['schedule_takeoff_date'] = flight.schedule_takeoff_date
                lived_flight_list.append(lived_flight)
            
            return lived_flight_list
        except:
            msg = traceback.format_exc()
            self.logger.error(msg)
            
            DBBase.Session.rollback()
            DBBase.Engine.dispose()
            
            return None
            
            
    def getCityList(self, lang):
        try:
            ret = CityInfo.find()
            
            city_list = []
            for one in ret:            
                city_list.append(one.city_zh)
            
            return city_list
        except:
            msg = traceback.format_exc()
            self.logger.error(msg)
            
            DBBase.Session.rollback()
            DBBase.Engine.dispose()
            
            return None
    
    
    def getAirlineList(self, lang):
        try:
            ret = AirlineInfo.find()
            
            airline_list = []
            for one in ret:
                hash = {} #@ReservedAssignment
                hash['takeoff_city'] = one.takeoff_city
                hash['arrival_city'] = one.arrival_city
                airline_list.append(hash)
            
            return airline_list
        except:
            msg = traceback.format_exc()
            self.logger.error(msg)
            
            DBBase.Session.rollback()
            DBBase.Engine.dispose()
            
            return None
    
    
    def getAirportList(self, lang):
        try:
            ret = AirportInfo.find()
            
            airport_info_list = []
            for one in ret:
                one_hash = {}
                one_hash['short'] = one.airport_short
                if lang == 'zh':
                    one_hash['full'] = one.airport_zh
                one_hash['city'] = self.getCityName(one.city, lang)
                
                airport_info_list.append(one_hash)
            
            return airport_info_list
        except:
            msg = traceback.format_exc()
            self.logger.error(msg)
            
            DBBase.Session.rollback()
            DBBase.Engine.dispose()
            
            return None
    
    
    def getCompanyName(self, short, lang):
        try:
            ret = CompanyInfo.find(company_short = short)
            
            if len(ret) == 1:
                if lang == 'zh':
                    return ret[0].company_zh   
            else:
                return ""
        except:
            msg = traceback.format_exc()
            self.logger.error(msg)
            
            DBBase.Session.rollback()
            DBBase.Engine.dispose()
            
            return None
        
        
    def getCityName(self, short, lang):
        try:
            ret = CityInfo.find(city_short = short)
            
            if len(ret) == 1:
                if lang == 'zh':
                    return ret[0].city_zh
            else:
                return ""
        except:
            msg = traceback.format_exc()
            self.logger.error(msg)
            
            DBBase.Session.rollback()
            DBBase.Engine.dispose()
            
            return None
    
    
    def getCityCode(self, short):
        try:
            ret = CityInfo.find(city_short = short)
            
            if len(ret) == 1:
                return ret[0].city_code
            else:
                return None
        except:
            msg = traceback.format_exc()
            self.logger.error(msg)
            
            DBBase.Session.rollback()
            DBBase.Engine.dispose()
            
            return None
        
    
    def getCityShort(self, name, lang):
        try:
            ret = None
            if lang == 'zh':
                ret = CityInfo.find(city_zh = name)
            
            if len(ret) == 1:
                return ret[0].city_short
            else:
                return ""
        except:
            msg = traceback.format_exc()
            self.logger.error(msg)
            
            DBBase.Session.rollback()
            DBBase.Engine.dispose()
            
            return None
        
    
    def getAirportName(self, short, lang):
        try:
            ret = AirportInfo.find(airport_short = short)
            
            if len(ret) == 1:
                if lang == 'zh':
                    return ret[0].airport_zh
            else:
                return ""
        except:
            msg = traceback.format_exc()
            self.logger.error(msg)
            
            DBBase.Session.rollback()
            DBBase.Engine.dispose()
            
            return None
    
    
    def getAirportCity(self, short):
        try:
            ret = AirportInfo.find(airport_short = short)
            
            if len(ret) == 1:
                return ret[0].city
            else:
                return None
        except:
            msg = traceback.format_exc()
            self.logger.error(msg)
            
            DBBase.Session.rollback()
            DBBase.Engine.dispose()
            
            return None
       
        
    def getPunctualityInfo(self, flight_no, takeoff_airport,  arrival_airport):
        try:
            ret = PunctualityInfo.find(flight_no = flight_no, takeoff_airport = takeoff_airport, arrival_airport = arrival_airport)
            
            punctuality_info = None
            if len(ret) == 1:
                punctuality_info = {}
                punctuality_info['on_time'] = ret[0].on_time
                punctuality_info['half_hour_late'] = ret[0].half_hour_late
                punctuality_info['one_hour_late'] = ret[0].one_hour_late
                punctuality_info['more_than_one_hour_late'] = ret[0].more_than_one_hour_late
                punctuality_info['cancel'] = ret[0].cancel
            
            return punctuality_info
        except:
                msg = traceback.format_exc()
                self.logger.error(msg)
                
                DBBase.Session.rollback()
                DBBase.Engine.dispose()
                
                return None
    
    
    def putPunctualityInfo(self, flight, punctualit_info):
        try:
            ret = PunctualityInfo.find(flight_no = flight['flight_no'], takeoff_airport = flight['takeoff_airport'], arrival_airport = flight['arrival_airport'])
            
            if len(ret) == 0:
                info = PunctualityInfo()
                
                info.flight_no = flight['flight_no']
                info.takeoff_airport = flight['takeoff_airport']
                info.arrival_airport = flight['arrival_airport']
                info.on_time = punctualit_info['on_time']
                info.half_hour_late = punctualit_info['half_hour_late']
                info.one_hour_late = punctualit_info['one_hour_late']
                info.more_than_one_hour_late = punctualit_info['more_than_one_hour_late']
                info.cancel = punctualit_info['cancel']
            
            info.add()
        except:
            msg = traceback.format_exc()
            self.logger.error(msg)
            
            DBBase.Session.rollback()
            DBBase.Engine.dispose()
            
            return None


    def putFlightFixInfo(self, flight_info_list):
        try:
            for one in flight_info_list:
                ret = FlightFixInfo.find(flight_no = one['flight_no'], schedule_takeoff_time = one['schedule_takeoff_time'], schedule_arrival_time = one['schedule_arrival_time'], schedule = one['schedule'])
                
                if len(ret) == 0:
                    flight_info = FlightFixInfo()
                    
                    flight_info.flight_no = one['flight_no']
                    flight_info.company = one['company']
                    flight_info.schedule_takeoff_time = one['schedule_takeoff_time']
                    flight_info.schedule_arrival_time = one['schedule_arrival_time']
                    flight_info.takeoff_city = one['takeoff_city']
                    flight_info.takeoff_airport = one['takeoff_airport']
                    flight_info.takeoff_airport_building = one['takeoff_airport_building']
                    flight_info.arrival_city = one['arrival_city']
                    flight_info.arrival_airport = one['arrival_airport']
                    flight_info.arrival_airport_building = one['arrival_airport_building']
                    flight_info.plane_model = one['plane_model']
                    flight_info.mileage = one['mileage']
                    flight_info.stopover = one['stopover']
                    flight_info.schedule = one['schedule']
                
                    flight_info.add()
        except:
            msg = traceback.format_exc()
            self.logger.error(msg)
            
            DBBase.Session.rollback()
            DBBase.Engine.dispose()
            
            return None
            
        
    def putAirportInfo(self, flight_info_list):
        try:
            for one in flight_info_list:
                ret = AirportInfo.find(airport_short = one['takeoff_airport_short'])
                if len(ret) == 0:
                    airport_info = AirportInfo()
                    airport_info.airport_short = one['takeoff_airport_short']
                    airport_info.airport_zh = one['takeoff_airport']
                    airport_info.add()
                    
                ret = AirportInfo.find(airport_short = one['arrival_airport_short'])
                if len(ret) == 0:
                    airport_info = AirportInfo()
                    airport_info.airport_short = one['arrival_airport_short']
                    airport_info.airport_zh = one['arrival_airport']
                    airport_info.add()
        except:
            msg = traceback.format_exc()
            self.logger.error(msg)
            
            DBBase.Session.rollback()
            DBBase.Engine.dispose()
            
            return None
            
    
    def putFlightRealtimeInfo(self, one):
        try:
            flight_info = FlightRealtimeInfo.find(flight_no = one['flight_no'], takeoff_airport = one['takeoff_airport'], arrival_airport = one['arrival_airport'], schedule_takeoff_date = one['schedule_takeoff_date'])
            
            if len(flight_info) == 0:
                flight_info = FlightRealtimeInfo()
            else:
                flight_info = flight_info[0]   
    
            flight_info.flight_no = one['flight_no']
            flight_info.flight_state = one['flight_state']
            flight_info.schedule_takeoff_time = one['schedule_takeoff_time']
            flight_info.estimate_takeoff_time = one['estimate_takeoff_time']
            flight_info.actual_takeoff_time = one['actual_takeoff_time']
            flight_info.schedule_arrival_time = one['schedule_arrival_time']
            flight_info.estimate_arrival_time = one['estimate_arrival_time']
            flight_info.actual_arrival_time = one['actual_arrival_time']
            flight_info.schedule_takeoff_date = one['schedule_takeoff_date']
            flight_info.takeoff_airport = one['takeoff_airport']
            flight_info.arrival_airport = one['arrival_airport']
            
            if one['actual_arrival_time'] != "--:--":
                flight_info.full_info = 1
        
            flight_info.add()
        except:
            msg = traceback.format_exc()
            self.logger.error(msg)
            
            DBBase.Session.rollback()
            DBBase.Engine.dispose()
            
            return None
    
    
    def putCompany(self, company_list):
        try:
            for one in company_list:
                ret = CompanyInfoAll.find(company_short = one[0])
                
                if len(ret) != 0:
                    continue
                
                company = CompanyInfoAll()
                company.company_short = one[0]
                company.company_zh = one[1]
                company.company_en = one[2]
                company.state = one[3]
                company.add()
        except:
            msg = traceback.format_exc()
            self.logger.error(msg)
            
            DBBase.Session.rollback()
            DBBase.Engine.dispose()
            
            return None

            
    def putAirline(self, airline_list):
        try:
            for one in airline_list:
                ret = one.split('-')
                
                airline = AirlineInfo()
                airline.takeoff_city = ret[0]
                airline.arrival_city = ret[1]
                airline.add()
        except:
            msg = traceback.format_exc()
            self.logger.error(msg)
            
            DBBase.Session.rollback()
            DBBase.Engine.dispose()
            
            return None
        
    
    def putFollowedInfo(self, device_token, followed_list):
        try:
            for one in followed_list:
                ret = FollowedInfo.findOne(device_token = ''.join(device_token.strip("<>").split(" ")),
                                        flight_no = one['flight_no'],
                                        takeoff_airport = one['takeoff_airport'],
                                        arrival_airport = one['arrival_airport'],
                                        schedule_takeoff_date = one['schedule_takeoff_date'])
                
                if ret is None:
                    info = FollowedInfo()
                    info.device_token = ''.join(device_token.strip("<>").split(" "))
                    info.flight_no = one['flight_no']
                    info.takeoff_airport = one['takeoff_airport']
                    info.arrival_airport = one['arrival_airport']
                    info.schedule_takeoff_date = one['schedule_takeoff_date']
                    
                    info.add()
        except:
            msg = traceback.format_exc()
            self.logger.error(msg)
            
            DBBase.Session.rollback()
            DBBase.Engine.dispose()
            
            return None
        
        
    def putPushInfo(self, push_candidate):
        try:
            ret = FollowedInfo.findOne(device_token = push_candidate['device_token'],
                                       flight_no = push_candidate['flight_no'],
                                       takeoff_airport = push_candidate['takeoff_airport'],
                                       arrival_airport = push_candidate['arrival_airport'],
                                       schedule_takeoff_date = push_candidate['schedule_takeoff_date'])
            
            if ret is not None:
                ret.push_switch = push_candidate['push_switch']
                ret.push_info = json.dumps(push_candidate['push_info'])
                ret.add()
        except:
            msg = traceback.format_exc()
            self.logger.error(msg)
            
            DBBase.Session.rollback()
            DBBase.Engine.dispose()
            
            return None
        
    
    def deleteFollowedInfo(self, device_token, followed_list):
        try:
            for one in followed_list:
                ret = FollowedInfo.findOne(device_token = ''.join(device_token.strip("<>").split(" ")),
                                        flight_no = one['flight_no'],
                                        takeoff_airport = one['takeoff_airport'],
                                        arrival_airport = one['arrival_airport'],
                                        schedule_takeoff_date = one['schedule_takeoff_date'])
                
                if ret is not None:
                    ret.delete()
        except:
            msg = traceback.format_exc()
            self.logger.error(msg)
            
            DBBase.Session.rollback()
            DBBase.Engine.dispose()
            
            return None
            
    
    def updateScheduleTimeInFlightFixInfo(self, flight):
        try:
            ret = FlightFixInfo.find(True, flight_no = flight['flight_no'], 
                                     takeoff_airport = flight['takeoff_airport'], 
                                     arrival_airport = flight['arrival_airport'])
            
            if ret is not None:
                ret.schedule_takeoff_time = flight['schedule_takeoff_time']
                ret.schedule_arrival_time = flight['schedule_arrival_time']
                
                ret.add()
        except:
            msg = traceback.format_exc()
            self.logger.error(msg)
            
            DBBase.Session.rollback()
            DBBase.Engine.dispose()
            
            return None
    
    
    def adjustFlightFixInfo(self):
        try:
            city_code_file = open("../../test/citycode", "r")
            
            hash = {} #@ReservedAssignment
            for line in city_code_file:
                line = line.strip()
                item = line.split('=')
                
                if len(item) == 2:
                    hash[item[1].decode("utf-8")] = item[0]
                    
            ret = CityInfo.find()
            
            for one in ret:
                if one.city_zh in hash:
                    one.city_code = hash[one.city_zh]
                    one.add()
    
            
            '''
            count = 0
            for one in ret:
                count += 1
                print count
                index = one.takeoff_airport.find('A')
                if index != -1:
                    one.takeoff_airport = one.takeoff_airport[:index]
                    one.takeoff_airport_building = 'A'
                
                index = one.takeoff_airport.find('B')
                if index != -1:
                    one.takeoff_airport = one.takeoff_airport[:index]
                    one.takeoff_airport_building = 'B'
                
                index = one.arrival_airport.find('A')
                if index != -1:
                    one.arrival_airport = one.arrival_airport[:index]
                    one.arrival_airport_building = 'A'
                
                index = one.arrival_airport.find('B')
                if index != -1:
                    one.arrival_airport = one.arrival_airport[:index]
                    one.arrival_airport_building = 'B'
                
                one.add()
            '''
        except:
            msg = traceback.format_exc()
            self.logger.error(msg)
            
            DBBase.Session.rollback()
            DBBase.Engine.dispose()
            
            return None
    
            
if __name__ == "__main__":
    init("root", "root", "127.0.0.1", "fd_db")
    
    db = DB()
    db.adjustFlightFixInfo()
    
            
    