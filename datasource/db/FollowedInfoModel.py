from sqlalchemy import Column
from sqlalchemy.types import * #@UnusedWildImport
from sqlalchemy import and_ #@UnusedImport
import datetime
import DBBase


class FollowedInfo(DBBase.Base):
    __tablename__ = 'followed_info_table'
    id = Column(INTEGER, primary_key = True) #@ReservedAssignment
    device_token = Column(VARCHAR(100), index = True, nullable = False)    
    flight_no = Column(VARCHAR(50), index = True, nullable = False)
    takeoff_airport = Column(VARCHAR(50), index = True, nullable = False)
    arrival_airport = Column(VARCHAR(50), index = True, nullable = False)
    schedule_takeoff_date = Column(VARCHAR(50), index = True, nullable = False)
    push_switch = Column(VARCHAR(2048), index = True, nullable = False, default = 'on')
    push_info = Column(VARCHAR(2048), nullable = False, default = '[]')
    date = Column(VARCHAR(20))
       
    
    def __init__(self, **kwargs):
        for k in kwargs:
            setattr(self, k, kwargs[k])


    @staticmethod    
    def findOne(**kwargs):
        session = DBBase.getSession()
        key_item = session.query(FollowedInfo).filter_by(**kwargs).first()
        DBBase.Session.remove()
        return key_item
    
    
    @staticmethod    
    def findAll(**kwargs):
        session = DBBase.getSession()
        key_item = session.query(FollowedInfo).filter_by(**kwargs).all()
        DBBase.Session.remove()
        return key_item
    
    
    @staticmethod    
    def count(**kwargs):
        session = DBBase.getSession()
        key_item = session.query(FollowedInfo).filter_by(**kwargs).count()
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



        
        
        
        
        
