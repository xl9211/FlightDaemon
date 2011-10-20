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

import random
import time
import json


def init(db_user, db_passwd, db_host, db_name):
        DBBase.Engine = create_engine("mysql://%s:%s@%s/%s?charset=utf8" %(db_user, db_passwd, db_host, db_name), pool_recycle = 3600, echo = False)
        DBBase.Base.metadata.create_all(DBBase.Engine)
        DBBase.Session = scoped_session(sessionmaker(bind = DBBase.Engine, expire_on_commit = False))


class DB:
    
    def __init__(self):
        pass
    
    
    def getFlightFixInfoByUniq(self, flight_no, takeoff_city, arrival_city, schedule_takeoff_date):
        ret = FlightFixInfo.find(flight_no = flight_no, takeoff_city = takeoff_city, arrival_city = arrival_city)
        week = time.strftime("%w", time.strptime(schedule_takeoff_date, "%Y-%m-%d"))
        flight_info_list = []
        for one in ret:
            if week in json.loads(one.schedule):
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
            if week in json.loads(one.schedule):
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
    
    
    def getFlightFixInfoByAirLine(self, takeoff_airport, arrival_airport, schedule_takeoff_date, company):
        week = time.strftime("%w", time.strptime(schedule_takeoff_date, "%Y-%m-%d"))
        ret = []
        if company == 'all':
            ret = FlightFixInfo.find(takeoff_airport = takeoff_airport, arrival_airport = arrival_airport)
        else:
            ret = FlightFixInfo.find(takeoff_airport = takeoff_airport, arrival_airport = arrival_airport, company = company)
        
        flight_info_list = []
        for one in ret:
            if week in json.loads(one.schedule):
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
            
            
    def getFlightRealtimeInfo(self, flight_no, schedule_takeoff_time, schedule_arrival_time, schedule_takeoff_date):
        ret = FlightRealtimeInfo.find(flight_no = flight_no, schedule_takeoff_time = schedule_takeoff_time, schedule_arrival_time = schedule_arrival_time, schedule_takeoff_date = schedule_takeoff_date)
        
        flight = {}
            
        flight['flight_no'] = flight_no
        flight['schedule_takeoff_time'] = schedule_takeoff_time
        flight['schedule_arrival_time'] = schedule_arrival_time
        flight['schedule_takeoff_date'] = schedule_takeoff_date
        flight['flight_state'] = "计划航班"
        flight['plane_model'] = ""
        flight['takeoff_airport_building'] = ""
        flight['estimate_takeoff_time'] = "--:--"
        flight['actual_takeoff_time'] = "--:--"
        flight['arrival_airport_building'] = ""
        flight['estimate_arrival_time'] = "--:--"
        flight['actual_arrival_time'] = "--:--"
        flight['flight_location'] = ""
        flight['company'] = ""
        flight['full_info'] = -1
        
        if len(ret) == 1:
            one = ret[0]
            
            flight['flight_state'] = one.flight_state
            flight['company'] = one.company
            flight['plane_model'] = one.plane_model
            flight['takeoff_airport_building'] = one.takeoff_airport_building
            flight['arrival_airport_building'] = one.arrival_airport_building
            flight['estimate_takeoff_time'] = one.estimate_takeoff_time
            flight['actual_takeoff_time'] = one.actual_takeoff_time
            flight['estimate_arrival_time'] = one.estimate_arrival_time
            flight['actual_arrival_time'] = one.actual_arrival_time
            flight['full_info'] = one.full_info
    
        return flight  

    
    def putFlightRealtimeInfo(self, flight_info_list):
        for one in flight_info_list: 
            flight_info = FlightRealtimeInfo.find(flight_no = one['flight_no'], schedule_takeoff_time = one['schedule_takeoff_time'], schedule_arrival_time = one['schedule_arrival_time'], schedule_takeoff_date = one['schedule_takeoff_date'])
            
            if len(flight_info) == 0:
                flight_info = FlightRealtimeInfo()
            else:
                flight_info = flight_info[0]   

            flight_info.flight_no = one['flight_no']
            flight_info.flight_state = one['flight_state']
            flight_info.company = one['company']
            flight_info.plane_model = one['plane_model'] 
            flight_info.takeoff_airport_building = one['takeoff_airport_building']
            flight_info.arrival_airport_building = one['arrival_airport_building']
            flight_info.schedule_takeoff_time = one['schedule_takeoff_time']
            flight_info.estimate_takeoff_time = one['estimate_takeoff_time']
            flight_info.actual_takeoff_time = one['actual_takeoff_time']
            flight_info.schedule_arrival_time = one['schedule_arrival_time']
            flight_info.estimate_arrival_time = one['estimate_arrival_time']
            flight_info.actual_arrival_time = one['actual_arrival_time']
            flight_info.schedule_takeoff_date = one['schedule_takeoff_date']
            
            if one['actual_arrival_time'] != '--:--':
                flight_info.full_info = 1
        
            flight_info.add()
            
            
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
    
    
    def getRandomFlight(self):
        flight_list = FlightFixInfo.getAllFlightNO()
        flight_no = flight_list[random.randint(0, len(flight_list))][0]
        
        return flight_no
    
    
    def getAllLivedFlight(self, date):
        flights = FlightRealtime.find(full_info = 0, schedule_takeoff_date = date)
        
        for flight in flights:
            flight.schedule_arrival_time
    
    
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
    
    
    def getCityList(self, lang = 'zh'):
        ret = CityInfo.find()
        
        city_list = []
        for one in ret:            
            city_list.append(one.city_zh)
        
        return city_list
    
    
    def getAirlineList(self, lang = 'zh'):
        ret = AirlineInfo.find()
        
        airline_list = []
        for one in ret:
            hash = {}
            hash['takeoff_city'] = one.takeoff_city
            hash['arrival_city'] = one.arrival_city
            airline_list.append(hash)
        
        return airline_list
            
      
    def putAirport(self, company_list):
        pass
    
    
    def adjustFlightFixInfo(self):
        ret = FlightFixInfo.find()
        
        '''
        count = 0
        for one in ret:
            num = FlightFixInfo.count(flight_no = one.flight_no, takeoff_city = one.takeoff_city, arrival_city = one.arrival_city)
            if num > 1:
                print num
                one.delete()
            count += 1
            print count           
        '''    
    
            
if __name__ == "__main__":
    init("root", "root", "127.0.0.1", "fd_db")
    
    db = DB()
    db.adjustFlightFixInfo()
    
            
    