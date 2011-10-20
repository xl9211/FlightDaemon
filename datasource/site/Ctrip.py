# coding=utf-8

'''
import os
import sys
current_path = os.getcwd()
base_path = os.path.abspath(os.path.join(current_path, "../../"))
sys.path.append(base_path)
'''
 

from Spider import Spider
import lxml.html.soupparser 


class Ctrip(Spider):
    CITY_CODE = {'北京':'BJS',
                 '上海':'SHA'}
    
    def __init__(self):
        Spider.__init__(self)
        
        self.ret_val = None
        
        
    def parse(self):
        doc = lxml.html.soupparser.fromstring(self.content)
        rows = doc.xpath("//li[@class='base_maincontent']//tr")
        self.ret_val = []
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
            
                self.ret_val.append(flight_info)
            
                
    def getFlightFixInfoByFlightNO(self, flight_no):
        self.url = "http://flights.ctrip.com/schedule/%s.html" % (flight_no)
        self.fetch()
        self.parse()
        
        return self.ret_val
    
    
    def getFlightFixInfoByAirline(self, takeoff_city, arrival_city):
        self.url = "http://flights.ctrip.com/schedule/%s.%s.html" % (self.CITY_CODE[takeoff_city], self.CITY_CODE[arrival_city])
        self.logger.info("fetch %s" % (self.url))
        self.fetch()
        self.parse()
        
        return self.ret_val
    
    
if __name__ == '__main__':     
    c = Ctrip()
    

    