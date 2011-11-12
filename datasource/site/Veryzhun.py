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
        
        
    def parseRealtimeInfo(self, flight, content):
        doc = lxml.html.soupparser.fromstring(self.content)
        
        numinfo = doc.xpath("//div[@class='numinfo']")
        numdap = doc.xpath("//div[@class='numdap']")
        numarr = doc.xpath("//div[@class='numarr']")
        
        flight_info_num = len(numinfo)
        for index in range(0, flight_info_num):
            schedule_takeoff_time = numdap[index].xpath("div[@class='numtime']//p")[0].text_content().strip()[-5:]
            schedule_arrival_time = numarr[index].xpath("div[@class='numtime']//p")[0].text_content().strip()[-5:]
            
            if schedule_takeoff_time != flight['schedule_takeoff_time'] or schedule_arrival_time != flight['schedule_arrival_time']:
                continue
            
            flight['flight_state'] = numinfo[index].xpath("div[@class='numtimestate']//span")[0].text_content().strip()
            
            flight_location = numinfo[index].xpath("ul//p[@class='planestate']//strong")
            if len(flight_location) != 0:
                flight['flight_location'] = flight_location[0].text_content().strip()
            else:
                flight['flight_location'] = ""

            flight['estimate_takeoff_time'] = numdap[index].xpath("div[@class='numtime']//p")[1].text_content().strip()[-5:]
            flight['actual_takeoff_time'] = numdap[index].xpath("div[@class='numtime']//p")[2].text_content().strip()[-5:]
            
            
            flight['estimate_arrival_time'] = numarr[index].xpath("div[@class='numtime']//p")[1].text_content().strip()[-5:]
            flight['actual_arrival_time'] = numarr[index].xpath("div[@class='numtime']//p")[2].text_content().strip()[-5:]
            
            if flight['actual_arrival_time'] != '--:--':
                flight['full_info'] = 1

    
    def getFlightRealTimeInfo(self, flight):
        try:
            url = "http://www.veryzhun.com/searchnum.asp?flightnum=%s" % (flight['flight_no'])
            content = self.fetch(url)
            if not (content < 0):
                self.parseRealtimeInfo(flight, content)
            else:
                return None
            
            return 0
        except:
            msg = traceback.format_exc()
            self.logger.error(msg)
            
            return None
    
    '''
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
    '''

    
if __name__ == '__main__':     
    v = Veryzhun()
    print "xxx"

    