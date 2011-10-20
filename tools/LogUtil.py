import logging
import os


class Logging:
    
    @staticmethod
    def getLogger(name = "FD", log_level = "debug", log_file = None):
        ## get log level.
        log_level = Logging.getLogLevel(log_level)

        ## create logger
        logger = logging.getLogger(name)
        
        ## create log handler.
        if len(logger.handlers) <= 0:
            if log_file is not None:
                handler = logging.FileHandler(log_file)
            else:
                handler = logging.StreamHandler()
                
            formatter = logging.Formatter("%(asctime)s\t%(process)d|%(thread)d\t%(levelname)s\t%(module)s\t%(funcName)s:%(lineno)d\t%(message)s", "%Y-%m-%d@%H:%M:%S")
            handler.setFormatter(formatter)
            
            logger.addHandler(handler)
            logger.setLevel(log_level)
        
        return logger


    @staticmethod
    def getLogLevel(log_level):
        log_level = getattr(logging, log_level.upper(), None)
        if log_level is None: raise Exception("No such log level.")
        return log_level


if __name__ == '__main__':
    logger = Logging.getLogger(log_level = "debug")
    
    logger.debug('debug message')
    logger.info('info message')
    logger.warn('warn message')
    logger.error('error message')
    logger.critical('critical message')
    
    
    logger.setLevel(Logging.getLogLevel("error"))
    
    logger.debug('debug message')
    logger.info('info message')
    logger.warn('warn message')
    logger.error('error message')
    logger.critical('critical message')
    
    ## The output looks like this:
    """
    2011-05-26@13:54:05	10006|-1215969600	DEBUG	testLogger	<module>:33	debug message
    2011-05-26@13:54:05	10006|-1215969600	INFO	testLogger	<module>:34	info message
    2011-05-26@13:54:05	10006|-1215969600	WARNING	testLogger	<module>:35	warn message
    2011-05-26@13:54:05	10006|-1215969600	ERROR	testLogger	<module>:36	error message
    2011-05-26@13:54:05	10006|-1215969600	CRITICAL	testLogger	<module>:37	critical message
    2011-05-26@13:54:05	10006|-1215969600	ERROR	testLogger	<module>:45	error message
    2011-05-26@13:54:05	10006|-1215969600	CRITICAL	testLogger	<module>:46	critical message
    """