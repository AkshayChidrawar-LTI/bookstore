import logging
import pytz
from datetime import datetime
from Executor import remove_script

class ISTFormatter(logging.Formatter):
    def converter(self,timestamp):
        dt = datetime.fromtimestamp(timestamp,pytz.timezone('Asia/Kolkata'))
        return dt
    def formatTime(self, record, datefmt=None):
        dt = self.converter(record.created)
        if datefmt is None:
            datefmt = '%Y-%m-%d %H:%M:%S'
        return dt.strftime(datefmt)

class LoggerManager:
    def __init__(
        self,
        logger_name,
        log_file,
        log_mode
    ):
        self.logger_name = logger_name
        self.log_file = log_file
        self.log_mode = log_mode
        self.logger = self.create_Logger()

    def create_Logger(self):
        logger = logging.getLogger(self.logger_name)
        logger.setLevel(logging.INFO)
        formatter = ISTFormatter('%(asctime)s - %(levelname)s - %(message)s')
        handler = logging.FileHandler(filename=self.log_file,mode=self.log_mode)
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        return logger

    def get_Handlers(self):
        return self.logger.handlers

    def remove_Logger(self):
        for handler in self.logger.handlers[:]: #this creates a shallow copy of pointers to the handler objects. This prevents damages to original copy (if any) during operation. 
            if isinstance(handler,logging.FileHandler): # drop only if FileHandler object
                remove_script(handler.baseFilename)
                self.logger.removeHandler(handler)
                handler.close()

# for logger_name in [
#             'logger_for_initializer',
#             'logger_for_scripting',
#             'logger_for_setup',
#             'logger_for_cleanup'
#         ]:



