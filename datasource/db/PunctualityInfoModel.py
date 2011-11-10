from sqlalchemy import Column
from sqlalchemy.types import * #@UnusedWildImport
from sqlalchemy import and_ #@UnusedImport
import datetime
import DBBase


class PunctualityInfo(DBBase.Base):
    __tablename__ = 'punctuality_info_table'
    flight_no = Column(VARCHAR(50), primary_key = True, index = True, nullable = False)
    takeoff_airport = Column(VARCHAR(50), primary_key = True, index = True, nullable = False)
    arrival_airport = Column(VARCHAR(50), primary_key = True, index = True, nullable = False)
    on_time = Column(VARCHAR(50))
    half_hour_late = Column(VARCHAR(50))
    one_hour_late = Column(VARCHAR(50))
    more_than_one_hour_late = Column(VARCHAR(50))
    cancel = Column(VARCHAR(50))
    date = Column(VARCHAR(20))
       
    
    def __init__(self, **kwargs):
        for k in kwargs:
            setattr(self, k, kwargs[k])
            
    
    @staticmethod    
    def find(**kwargs):
        session = DBBase.getSession()
        key_item = session.query(PunctualityInfo).filter_by(**kwargs).all()
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
        ret = self.find()
        if ret is not None:
            session.delete(ret)
        session.commit()
        DBBase.Session.remove()
        

    
        
        
