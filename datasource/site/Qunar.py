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
import json
from tools import LogUtil #@UnusedImport
import traceback 
import time


class Qunar(Spider):
    
    def __init__(self, config):
        Spider.__init__(self, config)
        
    
    def parseRealtimeInfo(self, flight, content):
        doc = lxml.html.soupparser.fromstring(content)
               
        content = doc.xpath("//div[@class='search_result']/dl[@class='state_detail']//span[@class='sd_2']/b/text()")
        
        if len(content) > 2:
            if content[0] == u"计划":
                flight['flight_state'] = u"计划航班"
            elif content[0] == u"起飞":
                flight['flight_state'] = u"已经起飞"
            elif content[0] == u"到达":
                flight['flight_state'] = u"已经到达"
            elif content[0] == u"取消":
                flight['flight_state'] = u"已经取消"
            elif content[0] == u"延误":
                flight['flight_state'] = u"已经延误"
            
            estimate_time = content[1].split('-')
            if len(estimate_time) == 2:
                if estimate_time[0].strip() != "":
                    flight['estimate_takeoff_time'] = estimate_time[0].strip()
                if estimate_time[1].strip() != "":
                    flight['estimate_arrival_time'] = estimate_time[1].strip()
            
            actual_time = content[2].split('-')
            if len(actual_time) == 2:
                if actual_time[0].strip() != "":
                    flight['actual_takeoff_time'] = actual_time[0].strip()
                if actual_time[1].strip() != "":
                    flight['actual_arrival_time'] = actual_time[1].strip()
                    flight['full_info'] = 1    
        else:
            return None
        
        return 0
            
       
    def getFlightRealTimeInfo(self, flight):
        try:
            url = "http://flight.qunar.com/schedule/fquery.jsp?flightCode=%s&d=%s&a=%s" % (flight['flight_no'], flight['takeoff_airport'], flight['arrival_airport'])
            content = self.fetch(url)
            if not (content < 0):
                return self.parseRealtimeInfo(flight, content)
            else:
                return None
        except:
            msg = traceback.format_exc()
            self.logger.error(msg)
            
            return None
           
            
    def parseFixInfo(self, content, takeoff_city, arrival_city):
        doc = lxml.html.soupparser.fromstring(content)
        rows = doc.xpath("//div[@class='result_content']/ul/li")
        ret_val = []
        mileage = doc.xpath("//div[@class='search_result']/p/em/text()")[0]
        for row in rows:
            flight_info = {}
            
            flight_info['flight_no'] = row.xpath("span[@class='c1']/span[@class='title']/b/a/text()")[0]
            flight_info['company'] = flight_info['flight_no'][:2]
            flight_info['schedule_takeoff_time'] = row.xpath("span[@class='c2']/text()")[0]
            flight_info['schedule_arrival_time'] = row.xpath("span[@class='c2']/em/text()")[0]
            flight_info['takeoff_city'] = takeoff_city
            flight_info['arrival_city'] = arrival_city
            
            temp_text = row.xpath("span[@class='c3']/text()")[0]
            find_index = temp_text.find('T')
            if find_index != -1:
                flight_info['takeoff_airport'] = temp_text[:find_index]
                flight_info['takeoff_airport_building'] = temp_text[find_index:]
            else:
                find_index = temp_text.find('A')
                if find_index != -1:
                    flight_info['takeoff_airport'] = temp_text[:find_index]
                    flight_info['takeoff_airport_building'] = 'A'
                else:
                    find_index = temp_text.find('B')
                    if find_index != -1:
                        flight_info['takeoff_airport'] = temp_text[:find_index]
                        flight_info['takeoff_airport_building'] = 'B'
                    else:
                        flight_info['takeoff_airport'] = temp_text
                        flight_info['takeoff_airport_building'] = ""
                
            temp_text = row.xpath("span[@class='c3']/text()")[1]
            find_index = temp_text.find('T')
            if find_index != -1:
                flight_info['arrival_airport'] = temp_text[:find_index]
                flight_info['arrival_airport_building'] = temp_text[find_index:]
            else:
                find_index = temp_text.find('A')
                if find_index != -1:
                    flight_info['arrival_airport'] = temp_text[:find_index]
                    flight_info['arrival_airport_building'] = 'A'
                else:
                    find_index = temp_text.find('B')
                    if find_index != -1:
                        flight_info['arrival_airport'] = temp_text[:find_index]
                        flight_info['arrival_airport_building'] = 'B'
                    else:
                        flight_info['arrival_airport'] = temp_text
                        flight_info['arrival_airport_building'] = ""
                
            flight_info['plane_model'] = row.xpath("span[@class='c1']/span[@class='title']/text()")[1]
            flight_info['mileage'] = mileage
            flight_info['stopover'] = row.xpath("span[@class='c6']/text()")[0]
            flight_info['schedule'] = row.xpath("span[@class='c5']/span[@class='circular_blue']/text()")
            
            href = row.xpath("span[@class='c1']/span[@class='title']/b/a/@href")[0]
            flight_info['takeoff_airport_short'] = href[href.find("&d=") + 3 : href.find("&d=") + 6]
            flight_info['arrival_airport_short'] = href[href.find("&a=") + 3 : href.find("&a=") + 6]
            
            flight_info['valid_date_from'] = row.xpath("span[@class='c7']/text()")[0].replace('.', '-')
            flight_info['valid_date_to'] = row.xpath("span[@class='c7']/text()")[1].replace('.', '-')
            
            ret_val.append(flight_info)
        return ret_val

    
    def getFlightFixInfoByAirline(self, takeoff_city, arrival_city):
        try:
            url = "http://flight.qunar.com/schedule/fsearch_list.jsp?departure=%s&arrival=%s" % (takeoff_city, arrival_city)
            content = self.fetch(url)
            if not (content < 0):
                return self.parseFixInfo(content, takeoff_city, arrival_city)
            else:
                return None
        except:
            msg = traceback.format_exc()
            self.logger.error(msg)
            
            return None
    
    
    '''
    def parseAirline(self):
        doc = lxml.html.soupparser.fromstring(self.content)
        airlines = doc.xpath("//ul[@class='apl_citylist fix']/li")
        
        self.ret_val = []
        for airline in airlines:
            self.ret_val.append(airline.text_content().strip())
        

    def getAirline(self, city):
        self.url = "http://flight.qunar.com/schedule/alph_order.jsp?city=%s" % (city)
        self.fetch()
        self.parseAirline()
        
        return self.ret_val
    '''
    
    
if __name__ == '__main__':     
    q = Qunar(None)
    
    print q.getFlightFixInfoByAirline(u'北京', u'杭州')
    '''
    flight = {}
    flight['flight_no'] = "CA1995"
    flight['takeoff_airport'] = "PEK"
    flight['arrival_airport'] = "SHA"
    flight['schedule_takeoff_date'] = "2011-11-14"
    print q.getFlightRealTimeInfo(flight)
    '''

    

    