import ConfigParser

class Config:
    
    def __init__(self, config_file):
        # get conf
        cf = ConfigParser.ConfigParser()
        cf.read(config_file)
        
        self.http_ip = cf.get('CherryPy', 'HttpIP')
        self.http_port = cf.get('CherryPy', 'HttpPort')
        self.thread_pool = cf.get('CherryPy', 'ThreadPool')
        self.request_queue_size = cf.get('CherryPy', 'RequestQueueSize')
        self.timeout = cf.get('CherryPy', 'Timeout')
        log_tmp = cf.get('CherryPy', 'LogOutput')
        self.log_output = False
        if log_tmp == 'yes':
            log_output = True
        self.log_error_file = cf.get('CherryPy', 'LogErrorFile')
        self.log_access_file = cf.get('CherryPy', 'LogAccessFile')
        
        self.db_host = cf.get("DB", "DBHost")
        self.db_user = cf.get("DB", "DBUser")
        self.db_passwd = cf.get("DB", "DBPasswd")
        self.db_name = cf.get("DB", "DBName")