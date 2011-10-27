from sqlalchemy import Column
from sqlalchemy.types import *
from sqlalchemy import and_
import datetime
import DBBase


class FlightRealtimeInfo(DBBase.Base):
    __tablename__ = 'flight_realtime_info_table'
    id = Column(INTEGER, primary_key = True)    
    flight_no = Column(VARCHAR(50), index = True, nullable = False)
    flight_state = Column(VARCHAR(50))
    estimate_takeoff_time = Column(VARCHAR(50))
    actual_takeoff_time = Column(VARCHAR(50))   
    estimate_arrival_time = Column(VARCHAR(50))
    actual_arrival_time = Column(VARCHAR(50))
    schedule_takeoff_time = Column(VARCHAR(50), index = True, nullable = False)
    schedule_arrival_time = Column(VARCHAR(50), index = True, nullable = False)
    takeoff_airport = Column(VARCHAR(50), index = True, nullable = False)
    arrival_airport = Column(VARCHAR(50), index = True, nullable = False)
    schedule_takeoff_date = Column(VARCHAR(50), index = True, nullable = False)
    full_info = Column(INTEGER, index = True, default = 0, nullable = False)
    date = Column(VARCHAR(20))

    
    def __init__(self, **kwargs):
        for k in kwargs:
            setattr(self, k, kwargs[k])

            
    @staticmethod    
    def find(one = False, **kwargs):
        session = DBBase.getSession()
        key_item = None
        if one:
            key_item = session.query(FlightRealtimeInfo).filter_by(**kwargs).one()
        else:
            key_item = session.query(FlightRealtimeInfo).filter_by(**kwargs).all()
        DBBase.Session.remove()
        return key_item
 

    @staticmethod    
    def getOneArrivedFlightNO():
        session = DBBase.getSession()
        key_item = session.query(FlightRealtimeInfo).filter_by(full_info = 1).order_by(FlightRealtimeInfo.date.desc()).first()
        DBBase.Session.remove()
        return key_item
    
    
    @staticmethod    
    def count(**kwargs):
        session = DBBase.getSession()
        key_item = session.query(FlightRealtimeInfo).filter_by(**kwargs).count()
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
        
        
        
        
        
