from sqlalchemy import Column
from sqlalchemy.types import *
from sqlalchemy import and_
import datetime
import DBBase


class CityInfo(DBBase.Base):
    __tablename__ = 'city_info_table'
    city_zh = Column(VARCHAR(100), primary_key = True, index = True)
    date = Column(VARCHAR(20))
       
    
    def __init__(self, **kwargs):
        for k in kwargs:
            setattr(self, k, kwargs[k])
            
    
    @staticmethod    
    def find(**kwargs):
        session = DBBase.getSession()
        key_item = session.query(CityInfo).filter_by(**kwargs).all()
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
        

    
        
        
