from sqlalchemy import Column
from sqlalchemy.types import * #@UnusedWildImport
from sqlalchemy import and_
import datetime
import DBBase


class FlightFixInfo(DBBase.Base):
    __tablename__ = 'flight_fix_info_table'
    id = Column(INTEGER, primary_key = True)    #@ReservedAssignment
    flight_no = Column(VARCHAR(50), index = True, nullable = False)
    company = Column(VARCHAR(50), index = True, nullable = False)
    schedule_takeoff_time = Column(VARCHAR(50), index = True, nullable = False)
    schedule_arrival_time = Column(VARCHAR(50), index = True, nullable = False)
    takeoff_city = Column(VARCHAR(50), index = True, nullable = False)
    takeoff_airport = Column(VARCHAR(50), index = True, nullable = False)
    takeoff_airport_building = Column(VARCHAR(50))
    arrival_city = Column(VARCHAR(50), index = True, nullable = False)
    arrival_airport = Column(VARCHAR(50), index = True, nullable = False)
    arrival_airport_building = Column(VARCHAR(50))
    plane_model = Column(VARCHAR(50), nullable = False)
    mileage = Column(VARCHAR(50))
    stopover = Column(VARCHAR(50))
    schedule = Column(VARCHAR(50), nullable = False)
    valid_date_from = Column(VARCHAR(20))
    valid_date_to = Column(VARCHAR(20))
    date = Column(VARCHAR(20))
       
    
    def __init__(self, **kwargs):
        for k in kwargs:
            setattr(self, k, kwargs[k])

         
    @staticmethod    
    def find(one = False, **kwargs):
        session = DBBase.getSession()
        key_item = None
        if one:
            key_item = session.query(FlightFixInfo).filter_by(**kwargs).first()
        else:
            key_item = session.query(FlightFixInfo).filter_by(**kwargs).all()
        DBBase.Session.remove()
        return key_item
    
    
    @staticmethod
    def findLike(flight_no):
        session = DBBase.getSession()
        key_item = None #@UnusedVariable
        key_item = session.query(FlightFixInfo).filter(FlightFixInfo.flight_no.like("%" + flight_no + "%")).all()
        DBBase.Session.remove()
        return key_item
    
    
    @staticmethod
    def findDelete(**kwargs):
        session = DBBase.getSession()
        key_item = session.query(FlightFixInfo).filter_by(**kwargs).delete()
        session.commit()
        DBBase.Session.remove()
        return key_item
 

    @staticmethod
    def getAllFlightNO():
        session = DBBase.getSession()
        key_item = session.query(FlightFixInfo.flight_no).all()
        DBBase.Session.remove()
        return key_item
    
    
    @staticmethod
    def getNowFlightNO(cur_time):
        session = DBBase.getSession()
        key_item = session.query(FlightFixInfo.flight_no).filter(and_(FlightFixInfo.schedule_takeoff_time <= cur_time, FlightFixInfo.schedule_arrival_time >= cur_time)).all()
        DBBase.Session.remove()
        return key_item
    
    
    @staticmethod
    def getOverdayRoute(cur_date):
        session = DBBase.getSession()
        key_item = session.query(FlightFixInfo.takeoff_airport, FlightFixInfo.arrival_airport).distinct().filter(FlightFixInfo.valid_date_to < cur_date).all()
        DBBase.Session.remove()
        return key_item
    
    
    @staticmethod    
    def count(**kwargs):
        session = DBBase.getSession()
        key_item = session.query(FlightFixInfo).filter_by(**kwargs).count()
        DBBase.Session.remove()
        return key_item
    
    
    def add(self):
        session = DBBase.getSession()
        self.date = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        session.add(self)
        session.commit()
        DBBase.Session.remove()
    
    
    def delete(self):
        session = DBBase.getSession()
        session.delete(self)
        session.commit()
        DBBase.Session.remove()
        
        
        
        
        
