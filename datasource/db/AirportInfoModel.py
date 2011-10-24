from sqlalchemy import Column
from sqlalchemy.types import *
from sqlalchemy import and_
import datetime
import DBBase


class AirportInfo(DBBase.Base):
    __tablename__ = 'airport_info_table'
    airport_short = Column(VARCHAR(50), primary_key = True, index = True)  
    airport_zh = Column(VARCHAR(100), nullable = False)
    airport_en = Column(VARCHAR(100))
    city = Column(VARCHAR(50), nullable = False)
    date = Column(VARCHAR(20))
       
    
    def __init__(self, **kwargs):
        for k in kwargs:
            setattr(self, k, kwargs[k])

           
    @staticmethod    
    def find(**kwargs):
        session = DBBase.getSession()
        key_item = session.query(AirportInfo).filter_by(**kwargs).all()
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
        

    
        
        
