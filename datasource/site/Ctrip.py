# coding=utf-8


import os
import sys
current_path = os.getcwd()
base_path = os.path.abspath(os.path.join(current_path, "../../"))
sys.path.append(base_path)

 

from Spider import Spider
import lxml.html.soupparser 
from lxml import etree


class Ctrip(Spider):
    CITY_CODE = {'北京':'BJS',
                 '上海':'SHA'}
    
    def __init__(self, config):
        Spider.__init__(self, config)
        
        
    def parse(self, content):
        doc = lxml.html.soupparser.fromstring(content)
        for one in doc.iter():
            one.text = "aaaaaaaaaaaaaa"
            
        print etree.tostring(doc, encoding = "utf-8")
        
        
        
        
        rows = doc.xpath("//li[@class='base_maincontent']//tr")
        ret_val = []
        '''
        for row in rows:
            flight_info = {}
            columns = row.xpath("td")
            
            if len(columns) > 0:
                flight_info['schedule_takeoff_time'] = columns[0].text_content().strip()
                flight_info['schedule_arrival_time'] = columns[1].text_content().strip()
                flight_info['flight_no'] = columns[2].text_content().strip()
                flight_info['company'] = columns[3].text_content().strip()
                flight_info['takeoff_city'] = columns[4].text_content().strip()
                flight_info['takeoff_airport'] = columns[5].text_content().strip()
                flight_info['arrival_city'] = columns[6].text_content().strip()
                flight_info['arrival_airport'] = columns[7].text_content().strip()
                flight_info['plane_model'] = columns[8].text_content().strip()
                flight_info['schedule'] = columns[9].text_content().strip()
            
                ret_val.append(flight_info)
        '''
                
        return ret_val
            
                
    def getFlightFixInfoByFlightNO(self, flight_no):
        url = "http://flights.ctrip.com/schedule/%s.html" % (flight_no)
        content = self.fetch(url)

        return self.parse(content)
    
    
    def getFlightFixInfoByAirline(self, takeoff_city, arrival_city):
        url = "http://flights.ctrip.com/schedule/%s.%s.html" % (self.CITY_CODE[takeoff_city], self.CITY_CODE[arrival_city])
        self.logger.info("fetch %s" % (url))
        content = self.fetch(url)

        return self.parse(content)
    
    
if __name__ == '__main__':     
    c = Ctrip(None)
    c.getFlightFixInfoByFlightNO("CA1509")
    

    