# coding=utf-8


import os
import sys
current_path = os.getcwd()
base_path = os.path.abspath(os.path.join(current_path, "../../"))
sys.path.append(base_path)



from Spider import Spider
import lxml.html.soupparser #@UnusedImport
import json


class Weather(Spider):
    
    def __init__(self, config):
        Spider.__init__(self, config)
        
        self.ret_val = {}
        
    def parseWeather(self):
        info = json.loads(self.content)
        
        realtime_info = {}
        realtime_info['temperature'] = info['weatherinfo']['temp']
        realtime_info['wind_direction'] = info['weatherinfo']['WD']
        realtime_info['wind_power'] = info['weatherinfo']['WSE']
        realtime_info['humidity'] = info['weatherinfo']['SD']
        realtime_info['update_time'] = info['weatherinfo']['time']

        self.ret_val['realtime'] = realtime_info
            
                
    def getWeather(self, city, wtype):
        if wtype == 'realtime' or wtype == 'all':
            self.url = "http://www.weather.com.cn/data/sk/%s.html" % (city)
            self.fetch()
            self.parseWeather()
            
        if wtype == 'days' or wtype == 'all':
            pass
        
        return self.ret_val
    
    
if __name__ == '__main__':     
    test = Weather(None)
    
    print test.getWeather('101011800', 'all')


    