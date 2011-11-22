from threading import Thread
from Queue import Queue
import traceback
import LogUtil


queues = {}


def getBGQueue(name, thread_num = 1):
    if name not in queues:
        queues[name] = BGQueue(thread_num) 
    return queues[name]


class BGQueue(object):
    
    def __init__(self, thread_num, retry_on_exception = False):
        self.logger = LogUtil.Logging.getLogger()
        
        self.thread_num = thread_num
        self.retry_on_exception = retry_on_exception
        self.queue = Queue()
        
        self.start()
    
    
    def start(self):
        self.logger.info("CALLED: thread_num = " + str(self.thread_num))
        for i in range(self.thread_num): #@UnusedVariable
            t = Thread(target = self.do)
            t.daemon = True
            t.start()

    
    def do(self):
        while True:
            ret = self.pop()
            try:
                (func, args, kwargs) = ret
                func(*args, **kwargs)
            except:
                self.logger.error(traceback.format_exc())
                if self.retryOnException:
                    self.push((func, args, kwargs))
            finally:
                self.queue.task_done()
    
    
    def pop(self):
        ret = self.queue.get(block = True)
        return ret
    
    
    def push(self, tar):
        try:
            self.queue.put(tar, block = True)
        except:
            msg = traceback.format_exc()
            self.logger.error(msg)


    def task(self, func):
        def newFunc(*args, **kwargs):
            self.push((func, args, kwargs))
        return newFunc
    
    
    def size(self):
        return self.queue.qsize()


