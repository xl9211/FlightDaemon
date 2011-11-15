from threading import Thread
import time

class Timer(Thread):
    def __init__(self, interval, function, once = False):
        Thread.__init__(self, None, "Timer", None)
        
        self.interval = interval
        self.do = function
        self.isPlay = True
        self.once = once
        
    
    def run(self):
        if self.once:
            self.do()
        else:
            while self.isPlay:
                time.sleep(self.interval)
                self.do()

    
    def stop(self):
        self.isPlay = False

