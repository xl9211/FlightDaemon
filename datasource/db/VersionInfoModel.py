from sqlalchemy import Column
from sqlalchemy.types import * #@UnusedWildImport
from sqlalchemy import and_ #@UnusedImport
import datetime
import DBBase


class VersionInfo(DBBase.Base):
    __tablename__ = 'version_info_table'
    id = Column(INTEGER, primary_key = True)    #@ReservedAssignment
    version = Column(VARCHAR(50))
    ipa = Column(VARCHAR(1024))
    changelog = Column(VARCHAR(4096))
    date = Column(VARCHAR(20))

    
    def __init__(self, **kwargs):
        for k in kwargs:
            setattr(self, k, kwargs[k])

            
    @staticmethod    
    def findOne(**kwargs):
        session = DBBase.getSession()
        key_item = session.query(VersionInfo).filter_by(**kwargs).first()
        DBBase.Session.remove()
        return key_item
    
    
    @staticmethod    
    def findNewest(**kwargs):
        session = DBBase.getSession()
        key_item = session.query(VersionInfo).filter_by(**kwargs).order_by(VersionInfo.version.desc()).first()
        DBBase.Session.remove()
        return key_item
    
    
    @staticmethod    
    def findAll(**kwargs):
        session = DBBase.getSession()
        key_item = session.query(VersionInfo).filter_by(**kwargs).order_by(VersionInfo.id.desc()).all()
        DBBase.Session.remove()
        return key_item
    
    
    @staticmethod    
    def count(**kwargs):
        session = DBBase.getSession()
        key_item = session.query(VersionInfo).filter_by(**kwargs).count()
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
        
        
        
        
        
