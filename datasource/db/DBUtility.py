# coding=utf-8

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm import scoped_session
import DBBase

from FlightFixInfoModel import FlightFixInfo
from CompanyInfoModel import CompanyInfo
from CompanyInfoAllModel import CompanyInfoAll
from FlightRealtimeInfoModel import FlightRealtimeInfo
from AirportInfoModel import AirportInfo
from AirlineInfoModel import AirlineInfo
from CityInfoModel import CityInfo
from PunctualityInfoModel import PunctualityInfo

import time
import json


def init(db_user, db_passwd, db_host, db_name):
        DBBase.Engine = create_engine("mysql://%s:%s@%s/%s?charset=utf8" %(db_user, db_passwd, db_host, db_name), pool_recycle = 3600, echo = False)
        DBBase.Base.metadata.create_all(DBBase.Engine)
        DBBase.Session = scoped_session(sessionmaker(bind = DBBase.Engine, expire_on_commit = False))


class DB:
    
    def __init__(self):
        pass
    
    
    def getFlightFixInfoByUniq(self, flight_no, takeoff_city, arrival_city, schedule_takeoff_date, lang):
        takeoff_city_short = self.getCityShort(takeoff_city, lang)
        arrival_city_short = self.getCityShort(arrival_city, lang)
        ret = FlightFixInfo.find(flight_no = flight_no, takeoff_city = takeoff_city_short, arrival_city = arrival_city_short)
        week = time.strftime("%w", time.strptime(schedule_takeoff_date, "%Y-%m-%d"))
        flight_info_list = []
        for one in ret:
            #if week in json.loads(one.schedule):
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
    
     
    def getFlightFixInfoByFlightNO(self, flight_no, schedule_takeoff_date):
        ret = FlightFixInfo.find(flight_no = flight_no)
        week = time.strftime("%w", time.strptime(schedule_takeoff_date, "%Y-%m-%d"))
        flight_info_list = []
        for one in ret:
            #if week in json.loads(one.schedule):
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
    
    
    def getFlightFixInfoByRoute(self, takeoff_airport, arrival_airport, schedule_takeoff_date, company):
        week = time.strftime("%w", time.strptime(schedule_takeoff_date, "%Y-%m-%d"))
        ret = []
        if company == 'all':
            ret = FlightFixInfo.find(takeoff_airport = takeoff_airport, arrival_airport = arrival_airport)
        else:
            ret = FlightFixInfo.find(takeoff_airport = takeoff_airport, arrival_airport = arrival_airport, company = company)
        
        flight_info_list = []
        for one in ret:
            #if week in json.loads(one.schedule):
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
            
            
    def getFlightRealtimeInfo(self, flight):
        ret = FlightRealtimeInfo.find(flight_no = flight['flight_no'], 
                                      schedule_takeoff_time = flight['schedule_takeoff_time'], 
                                      schedule_arrival_time = flight['schedule_arrival_time'], 
                                      schedule_takeoff_date = flight['schedule_takeoff_date'])
        
        if len(ret) >= 1:
            one = ret[0]
            
            flight['flight_state'] = one.flight_state
            flight['estimate_takeoff_time'] = one.estimate_takeoff_time
            flight['actual_takeoff_time'] = one.actual_takeoff_time
            flight['estimate_arrival_time'] = one.estimate_arrival_time
            flight['actual_arrival_time'] = one.actual_arrival_time
            flight['full_info'] = one.full_info
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
    
        return flight
    
    
    def getFlightList(self):
        ret = FlightFixInfo.find()
        
        flight_info_list = []
        for one in ret:
            one_hash = {}
            
            one_hash['flight_no'] = one.flight_no
            one_hash['takeoff_airport'] = one.takeoff_airport
            one_hash['arrival_airport'] = one.arrival_airport
            
            flight_info_list.append(one_hash)
        
        return flight_info_list
            

    def getCompanyList(self, lang):
        ret = CompanyInfo.find()
        
        company_info_list = []
        for one in ret:
            one_hash = {}
            one_hash['short'] = one.company_short
            if lang == 'zh':
                one_hash['full'] = one.company_zh
            
            company_info_list.append(one_hash)
        
        return company_info_list
    
    
    def getRandomFlightList(self, cur_time):
        ret = FlightFixInfo.getNowFlightNO(cur_time)
        
        flight_list = []
        for one in ret:
            flight_list.append(one[0])
        
        return flight_list


    def getRandomFlight(self):
        ret = FlightRealtimeInfo.getOneArrivedFlightNO()
        
        flight = None
        
        if ret is not None:
            flight = {}
            flight['flight_no'] = ret.flight_no
            flight['schedule_takeoff_date'] = ret.schedule_takeoff_date
            
        return flight
        
    
    def getAllLivedFlight(self):
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
            
            
    def getCityList(self, lang):
        ret = CityInfo.find()
        
        city_list = []
        for one in ret:            
            city_list.append(one.city_zh)
        
        return city_list
    
    
    def getAirlineList(self, lang):
        ret = AirlineInfo.find()
        
        airline_list = []
        for one in ret:
            hash = {}
            hash['takeoff_city'] = one.takeoff_city
            hash['arrival_city'] = one.arrival_city
            airline_list.append(hash)
        
        return airline_list
    
    
    def getAirportList(self, lang):
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
    
    
    def getCompanyName(self, short, lang):
        ret = CompanyInfo.find(company_short = short)
        
        if len(ret) == 1:
            if lang == 'zh':
                return ret[0].company_zh   
        else:
            return ""
        
        
    def getCityName(self, short, lang):
        ret = CityInfo.find(city_short = short)
        
        if len(ret) == 1:
            if lang == 'zh':
                return ret[0].city_zh
        else:
            return ""
    
    
    def getCityCode(self, short):
        ret = CityInfo.find(city_short = short)
        
        if len(ret) == 1:
            return ret[0].city_code
        else:
            return None
        
    
    def getCityShort(self, name, lang):
        ret = None
        if lang == 'zh':
            ret = CityInfo.find(city_zh = name)
        
        if len(ret) == 1:
            return ret[0].city_short
        else:
            return ""
        
    
    def getAirportName(self, short, lang):
        ret = AirportInfo.find(airport_short = short)
        
        if len(ret) == 1:
            if lang == 'zh':
                return ret[0].airport_zh
        else:
            return ""
    
    
    def getAirportCity(self, short):
        ret = AirportInfo.find(airport_short = short)
        
        if len(ret) == 1:
            return ret[0].city
        else:
            return None
       
        
    def getPunctualityInfo(self, flight_no, takeoff_airport,  arrival_airport):
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
    
    
    def putPunctualityInfo(self, flight, punctualit_info):
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


    def putFlightFixInfo(self, flight_info_list):
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
            
        
    def putAirportInfo(self, flight_info_list):
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

    
    def putFlightRealtimeInfo(self, one):
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
    
    
    def putCompany(self, company_list):
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

            
    def putAirline(self, airline_list):
        for one in airline_list:
            ret = one.split('-')
            
            airline = AirlineInfo()
            airline.takeoff_city = ret[0]
            airline.arrival_city = ret[1]
            airline.add()
            
    
    def updateScheduleTimeInFlightFixInfo(self, flight):
        ret = FlightFixInfo.find(True, flight_no = flight['flight_no'], 
                                 takeoff_airport = flight['takeoff_airport'], 
                                 arrival_airport = flight['arrival_airport'])
        
        if ret is not None:
            ret.schedule_takeoff_time = flight['schedule_takeoff_time']
            ret.schedule_arrival_time = flight['schedule_arrival_time']
            
            ret.add()
        
    
    
    def adjustFlightFixInfo(self):
        city_code_file = open("../../test/citycode", "r")
        
        hash = {}
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
    
            
if __name__ == "__main__":
    init("root", "root", "127.0.0.1", "fd_db")
    
    db = DB()
    db.adjustFlightFixInfo()
    
            
    