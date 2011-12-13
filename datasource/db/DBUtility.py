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
from VersionInfoModel import VersionInfo

import traceback
from tools import LogUtil
import json
import datetime


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
        
    
    ##############################################################################################
    # FlightFixInfo
    def getFlightFixInfoByFlightNO(self, flight_no):
        try:
            ret = FlightFixInfo.findLike(flight_no)
            return self.__convertFixInfo(ret)
        except:
            msg = traceback.format_exc()
            self.logger.error(msg)
            
            DBBase.Session.rollback()
            DBBase.Engine.dispose()
            
            return None
    

    def getFlightFixInfoByRoute(self, takeoff_airport, arrival_airport, company):
        try:
            ret = []
            if company == 'all':
                ret = FlightFixInfo.find(takeoff_airport = takeoff_airport, arrival_airport = arrival_airport)
            else:
                ret = FlightFixInfo.find(takeoff_airport = takeoff_airport, arrival_airport = arrival_airport, company = company)
            
            return self.__convertFixInfo(ret)
        except:
            msg = traceback.format_exc()
            self.logger.error(msg)
            
            DBBase.Session.rollback()
            DBBase.Engine.dispose()
            
            return None
        
        
    def getFlightFixInfoByUniq(self, flight_no, takeoff_airport, arrival_airport):
        try:
            ret = FlightFixInfo.find(flight_no = flight_no, takeoff_airport = takeoff_airport, arrival_airport = arrival_airport)
            return self.__convertFixInfo(ret)
        except:
            msg = traceback.format_exc()
            self.logger.error(msg)
            
            DBBase.Session.rollback()
            DBBase.Engine.dispose()
            
            return None
        
    
    def getOverdayRoute(self, cur_date):
        try:
            ret = FlightFixInfo.getOverdayRoute(cur_date)
            
            route_list = []
            for one in ret:
                one_hash = {}
                
                one_hash['takeoff_airport'] = one.takeoff_airport
                one_hash['arrival_airport'] = one.arrival_airport
                
                route_list.append(one_hash)
            
            return route_list
        except:
            msg = traceback.format_exc()
            self.logger.error(msg)
            
            DBBase.Session.rollback()
            DBBase.Engine.dispose()
            
            return None
        
    
    def __convertFixInfo(self, data):
        flight_info_list = []
        for one in data:
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
            one_hash['stopover'] = one.stopover
            one_hash['schedule'] = json.loads(one.schedule)
            one_hash['valid_date_from'] = one.valid_date_from
            one_hash['valid_date_to'] = one.valid_date_to
            
            flight_info_list.append(one_hash)
        
        return flight_info_list
    
    
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
        
    
    def putFlightFixInfo(self, flight_info_list):
        try:
            for one in flight_info_list:
                flight_info = FlightFixInfo()
                
                flight_info.flight_no = one['flight_no']
                flight_info.company = one['company']
                flight_info.schedule_takeoff_time = one['schedule_takeoff_time']
                flight_info.schedule_arrival_time = one['schedule_arrival_time']
                flight_info.takeoff_airport = self.getAirportShort(one['takeoff_airport'].encode("utf-8"), 'zh')
                flight_info.takeoff_city = self.getAirportCity(flight_info.takeoff_airport)
                flight_info.takeoff_airport_building = one['takeoff_airport_building']
                flight_info.arrival_airport = self.getAirportShort(one['arrival_airport'].encode("utf-8"), 'zh')
                flight_info.arrival_city = self.getAirportCity(flight_info.arrival_airport)        
                flight_info.arrival_airport_building = one['arrival_airport_building']
                flight_info.plane_model = one['plane_model'].encode("utf-8")
                flight_info.mileage = one['mileage']
                flight_info.stopover = one['stopover']
                flight_info.schedule = json.dumps(one['schedule'])
                flight_info.valid_date_from = one['valid_date_from']
                flight_info.valid_date_to = one['valid_date_to']
            
                flight_info.add()
            self.logger.info("%s rows is added" % (str(len(flight_info_list))))
        except:
            msg = traceback.format_exc()
            self.logger.error(msg)
            
            DBBase.Session.rollback()
            DBBase.Engine.dispose()
            
            return None
        
    
    def checkRouteInfo(self):
        try:
            ret = FlightFixInfo.getAllRoute()
            
            route_info = []
            for one in ret:
                route = {}
                route["takeoff_airport"] = one.takeoff_airport
                route["arrival_airport"] = one.arrival_airport
                count = len(FlightFixInfo.find(takeoff_airport = one.takeoff_airport, arrival_airport = one.arrival_airport))
                if count == 0:
                    route["flight_count"] = "ZERO"
                else:
                    route["flight_count"] = count
                route_info.append(route)
            
            return route_info
        except:
            msg = traceback.format_exc()
            self.logger.error(msg)
            
            DBBase.Session.rollback()
            DBBase.Engine.dispose()
            
            return None
        
    
    def deleteRoute(self, takeoff_airport, arrival_airport):
        try:
            self.logger.info("delete route %s %s" % (takeoff_airport, arrival_airport))
            ret = FlightFixInfo.findDelete(takeoff_airport = takeoff_airport, arrival_airport = arrival_airport)
            self.logger.info("%s rows is deleted" % (str(ret)))
            
            return 0
        except:
            msg = traceback.format_exc()
            self.logger.error(msg)
            
            DBBase.Session.rollback()
            DBBase.Engine.dispose()
            
            return None
    # FlightFixInfo
    ##############################################################################################
    

    ##############################################################################################
    # FlightRealtimeInfo        
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
        
            return flight
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
        
    
    def getLivedFlight(self):
        try:
            day = []
            day.append(datetime.datetime.now().strftime("%Y-%m-%d"))
            day.append((datetime.datetime.now() - datetime.timedelta(1)).strftime("%Y-%m-%d"))
            
            lived_flight_list = []
            for one in day:
                flights = FlightRealtimeInfo.find(full_info = 0, schedule_takeoff_date = one) 
                for flight in flights:
                    lived_flight = {}
                    lived_flight['flight_no'] = flight.flight_no
                    lived_flight['schedule_takeoff_time'] = flight.schedule_takeoff_time
                    lived_flight['schedule_arrival_time'] = flight.schedule_arrival_time
                    lived_flight['takeoff_airport'] = flight.takeoff_airport
                    lived_flight['arrival_airport'] = flight.arrival_airport
                    lived_flight['schedule_takeoff_date'] = flight.schedule_takeoff_date
                    lived_flight['flight_state'] = flight.flight_state
                    lived_flight['estimate_takeoff_time'] = flight.estimate_takeoff_time
                    lived_flight['actual_takeoff_time'] = flight.actual_takeoff_time
                    lived_flight['estimate_arrival_time'] = flight.estimate_arrival_time
                    lived_flight['actual_arrival_time'] = flight.actual_arrival_time
                    lived_flight['full_info'] = flight.full_info
                    lived_flight_list.append(lived_flight)
            
            return lived_flight_list
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
    # FlightRealtimeInfo
    ##############################################################################################
    
    
    ##############################################################################################
    # FollowedInfo
    def getPushCandidate(self, flight):
        try:
            ret = FollowedInfo.findAll(push_switch = 'on', 
                                       flight_no = flight['flight_no'], 
                                       takeoff_airport = flight['takeoff_airport'],
                                       arrival_airport = flight['arrival_airport'],
                                       schedule_takeoff_date = flight['schedule_takeoff_date'])
            
            push_list = []
            for one in ret:
                one_hash = {}
                one_hash['device_token'] = one.device_token
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
    
    
    def getPushInfoList(self, device_token, push_switch):
        try:
            ret = []
            if device_token != "" and push_switch != "":
                ret = FollowedInfo.findAll(device_token = device_token, push_switch = push_switch)
            elif device_token == "" and push_switch == "":
                ret = FollowedInfo.findAll()
            elif device_token != "" and push_switch == "":
                ret = FollowedInfo.findAll(device_token = device_token)
            elif device_token == "" and push_switch != "":
                ret = FollowedInfo.findAll(push_switch = push_switch)
            
            push_list = []
            for one in ret:
                one_hash = {}
                one_hash['device_token'] = one.device_token
                one_hash['flight'] = "[%s][%s][%s][%s]" % (one.flight_no, one.takeoff_airport, one.arrival_airport, one.schedule_takeoff_date)
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
        
        
    def putPushInfo(self, push_candidate, flight):
        try:
            ret = FollowedInfo.findOne(device_token = push_candidate['device_token'],
                                       flight_no = flight['flight_no'],
                                       takeoff_airport = flight['takeoff_airport'],
                                       arrival_airport = flight['arrival_airport'],
                                       schedule_takeoff_date = flight['schedule_takeoff_date'])
            
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
    # FollowedInfo
    ##############################################################################################
    

    ##############################################################################################
    # CompanyInfo
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
    # CompanyInfo
    ##############################################################################################
    
    
    ##############################################################################################       
    # CityInfo
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
    # CityInfo
    ##############################################################################################
    
    
    ##############################################################################################
    # AirlineInfo
    def getAirlineList(self):
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
    # AirlineInfo
    ##############################################################################################
    
    
    ##############################################################################################
    # AirportInfo
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
        
    
    def getAirportShort(self, name, lang):
        try:
            ret = None
            if lang == 'zh':
                ret = AirportInfo.find(airport_zh = name)
            
            if len(ret) == 1:
                return ret[0].airport_short
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
    # AirportInfo
    ##############################################################################################
    
    
    ##############################################################################################
    # CompanyInfo
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
    # CompanyInfo
    ##############################################################################################
    

    ##############################################################################################
    # PunctualityInfo
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
    # PunctualityInfo
    ############################################################################################## 
    
    
    ##############################################################################################
    # VersionInfo
    def getVersionInfoList(self):
        try:
            ret = VersionInfo.findAll()
            
            version_list = []
            for one in ret:
                one_hash = {}
                one_hash['id'] = one.id
                one_hash['version'] = one.version
                one_hash['ipa'] = one.ipa
                one_hash['changelog'] = one.changelog.replace("\n", "<br>")
                version_list.append(one_hash)
            
            return version_list
        except:
                msg = traceback.format_exc()
                self.logger.error(msg)
                
                DBBase.Session.rollback()
                DBBase.Engine.dispose()
                
                return None
            
    
    def putVersionInfo(self, version, ipa, changelog):
        try:
            info = VersionInfo()
            
            info.version = version
            info.ipa = ipa
            info.changelog = changelog
            
            info.add()
        except:
            msg = traceback.format_exc()
            self.logger.error(msg)
            
            DBBase.Session.rollback()
            DBBase.Engine.dispose()
            
            return None
        
    
    def getNewestVersionInfo(self):
        try:
            ret = VersionInfo.findNewest()
            
            one_hash = {}
            if ret is not None:
                one_hash['version'] = ret.version
                one_hash['ipa'] = ret.ipa
                one_hash['changelog'] = ret.changelog
            return one_hash
        except:
                msg = traceback.format_exc()
                self.logger.error(msg)
                
                DBBase.Session.rollback()
                DBBase.Engine.dispose()
                
                return None
    # VersionInfo
    ############################################################################################## 


    ############################################################################################## 
    # Test
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
    # Test
    ############################################################################################## 

            
if __name__ == "__main__":
    init("root", "root", "127.0.0.1", "fd_db")
    
    db = DB()

    ret = db.deleteRoute("PEK", "HGH")

    
            
    