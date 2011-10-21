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
from tools import LogUtil
import traceback 


class Veryzhun(Spider):
    
    def __init__(self, config):
        Spider.__init__(self, config)
        self.logger = LogUtil.Logging.getLogger()
        self.ret_val = None
        
        
    def parse(self, schedule_takeoff_date):
        doc = lxml.html.soupparser.fromstring(self.content)
        
        self.ret_val = []
        
        numinfo = doc.xpath("//div[@class='numinfo']")
        numdap = doc.xpath("//div[@class='numdap']")
        numarr = doc.xpath("//div[@class='numarr']")
        
        flight_info_num = len(numinfo)
        for index in range(0, flight_info_num):
            flight_info = {}
            
            flight_info['flight_no'] = numinfo[index].xpath("ul//li[@class='num_here']")[0].text_content().strip()
            flight_info['flight_state'] = numinfo[index].xpath("div[@class='numtimestate']//span")[0].text_content().strip()
            flight_info['company'] = numinfo[index].xpath("ul//td[@class='airname']//a")[0].text_content().strip()
            flight_info['plane_model'] = numinfo[index].xpath("ul//td[@class='airname']//a")[1].text_content().strip()
            
            flight_location = numinfo[index].xpath("ul//p[@class='planestate']//strong")
            if len(flight_location) != 0:
                flight_info['flight_location'] = flight_location[0].text_content().strip()
            else:
                flight_info['flight_location'] = ""
            
            temp_text = numdap[index].xpath("ul//li[@class='num_here']")[0].text_content().strip()
            find_index = temp_text.find('T')
            if find_index != -1:
                flight_info['takeoff_airport_building'] = temp_text[find_index:]
            else:
                flight_info['takeoff_airport_building'] = ""
            flight_info['schedule_takeoff_time'] = numdap[index].xpath("div[@class='numtime']//p")[0].text_content().strip()[-5:]
            flight_info['estimate_takeoff_time'] = numdap[index].xpath("div[@class='numtime']//p")[1].text_content().strip()[-5:]
            flight_info['actual_takeoff_time'] = numdap[index].xpath("div[@class='numtime']//p")[2].text_content().strip()[-5:]
            #flight_info['takeoff_delay_advance_time'] = self.computeIntervalTime(flight_info['schedule_takeoff_time'], flight_info['actual_takeoff_time'])
            
            temp_text = numarr[index].xpath("ul//li[@class='num_here']")[0].text_content().strip()
            find_index = temp_text.find('T')
            if find_index != -1:
                flight_info['arrival_airport_building'] = temp_text[find_index:]
            else:
                flight_info['arrival_airport_building'] = ""  
            flight_info['schedule_arrival_time'] = numarr[index].xpath("div[@class='numtime']//p")[0].text_content().strip()[-5:]
            flight_info['estimate_arrival_time'] = numarr[index].xpath("div[@class='numtime']//p")[1].text_content().strip()[-5:]
            flight_info['actual_arrival_time'] = numarr[index].xpath("div[@class='numtime']//p")[2].text_content().strip()[-5:]
            #flight_info['arrival_delay_advance_time'] = self.computeIntervalTime(flight_info['schedule_arrival_time'], flight_info['actual_arrival_time'])
            flight_info['schedule_takeoff_date'] = schedule_takeoff_date
            
            self.ret_val.append(flight_info)

    
    def getFlightRealTimeInfo(self, flight_no, schedule_takeoff_date):
        try:
            self.url = "http://www.veryzhun.com/searchnum.asp?flightnum=%s" % (flight_no)
            if self.fetch() != -1:
                self.parse(schedule_takeoff_date)
            
            return self.ret_val
        except:
            msg = traceback.format_exc()
            self.logger.error(msg)
            
            return None
    
    
    def computeIntervalTime(self, start_time, end_time):
        interval_time = "+00：00"
        
        if start_time != "--:--" and end_time != "--:--":
            start_time_high = int(start_time[:2])
            start_time_low = int(start_time[-2:])
            end_time_high = int(end_time[:2])
            end_time_low = int(end_time[-2:])
                
            if start_time <= end_time:
                if end_time_low < start_time_low:
                    end_time_high -= 1
                    end_time_low += 60
                interval_time_high = end_time_high - start_time_high
                interval_time_low = end_time_low - start_time_low
                interval_time = "+%02d:%02d" % (interval_time_high, interval_time_low)
                
            if start_time > end_time:
                if start_time_high - end_time_high < 2:
                    if start_time_high > end_time_high:
                        start_time_high -= 1
                        start_time_low += 60
                    interval_time_high = start_time_high - end_time_high
                    interval_time_low = start_time_low - end_time_low
                    interval_time = "-%02d:%02d" % (interval_time_high, interval_time_low)
                else:
                    end_time_high += 24
                    if end_time_low < start_time_low:
                        end_time_high -= 1
                        end_time_low += 60
                    interval_time_high = end_time_high - start_time_high
                    interval_time_low = end_time_low - start_time_low
                    interval_time = "+%02d:%02d" % (interval_time_high, interval_time_low)
            
        return interval_time

    
if __name__ == '__main__':     
    v = Veryzhun()
    print "xxx"

    