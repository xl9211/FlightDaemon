from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()
Engine = None
Session = None

def getSession():
    session = Session()
    return session