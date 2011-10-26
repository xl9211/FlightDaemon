# coding=utf-8


import os
import sys
current_path = os.getcwd()
base_path = os.path.abspath(os.path.join(current_path, "../../"))
sys.path.append(base_path)

 

from Spider import Spider
import lxml.html.soupparser 
import json
from tools import LogUtil
import traceback 
import time


class Feeyo(Spider):
    
    def __init__(self, config):
        Spider.__init__(self, config)
        
    
    def parsePunctualityInfo(self):
        doc = lxml.html.soupparser.fromstring(self.content)
        content = doc.xpath("//div[@class='Pblock2']/table")
        
        punctuality_info = {}
        punctuality_info['on_time'] = ""
        punctuality_info['half_hour_late'] = ""
        punctuality_info['one_hour_late'] = ""
        punctuality_info['more_than_one_hour_late'] = ""
        punctuality_info['cancel'] = ""
            
        if len(content) > 3:    
            rows = content[3].xpath("tr")

            punctuality_info['on_time'] = rows[0].xpath("td/text()")[1]
            punctuality_info['half_hour_late'] = rows[1].xpath("td/text()")[1]
            punctuality_info['one_hour_late'] = rows[2].xpath("td/text()")[1]
            punctuality_info['more_than_one_hour_late'] = rows[3].xpath("td/text()")[1]
            punctuality_info['cancel'] = rows[4].xpath("td/text()")[1]
        
        return punctuality_info
        
       
    def getPunctualityInfo(self, takeoff_airport, arrival_airport, flight_no):
        try:
            self.url = "http://www.feeyo.com/vflight/delay/a/%s_%s_%s.htm" % (takeoff_airport, arrival_airport, flight_no)
            if self.fetch() == 0:
                return self.parsePunctualityInfo()
            else:
                return None
        except:
            msg = traceback.format_exc()
            self.logger.error(msg)
            
            return None

    
if __name__ == '__main__':     
    f = Feeyo(None)
    
    punctuality_info = f.getPunctualityInfo('KMG', 'CTU', 'ZH8091')
    print punctuality_info
    

    