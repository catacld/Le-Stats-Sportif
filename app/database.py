import threading
import logging
import time
from logging.handlers import RotatingFileHandler
from queue import Queue


class Database:
    _self = None


    def __new__(cls):
        if cls._self is None:
            cls._self = super().__new__(cls)
            cls.records = []
            cls.job_status = {}
            cls.shutdown = False
            cls.job_id = 1

            # all the tasks received from requests
            cls.tasks = Queue()

            # lock needed for the synchronized methods
            cls.jobLock = threading.Lock()

            # logger setup
            cls._self.logger = logging.getLogger(__name__)
            cls._self.logger.setLevel(logging.INFO)

            handler = RotatingFileHandler('webserver.log',
                                          mode='a', maxBytes=110000, backupCount=10)
            formatter = logging.Formatter('%(asctime)s %(levelname)-8s'
                                          '%(name)-15s:%(lineno)4s: %(message)-80s',
                                          datefmt='%Y-%m-%d %H:%M:%S')
            formatter.converter = time.gmtime
            handler.setFormatter(formatter)

            cls._self.logger.addHandler(handler)

        return cls._self

    # helper method used to set the records
    # with the values read from the csv
    def set_records(self, records):
        self.records = records

    # method used to assign a job id
    # when a request is received
    # no need to be synchronized since
    # only the main thread will call it
    def assign_job_id(self):

        self.job_id += 1

        return self.job_id

    # synchronized methods to access the dictionary
    # where each job's status is stored
    # to make sure the value that is read
    # will always be correct

    def set_job_status(self, job_id, status):

        self.jobLock.acquire()

        self.job_status[job_id] = status

        self.jobLock.release()

    def get_job_status(self, job_id):
        self.jobLock.acquire()

        if job_id not in self.job_status:
            self.jobLock.release()
            return 'invalid id'

        status = self.job_status[job_id]

        self.jobLock.release()

        return status

    def get_jobs_left(self):

        jobs_left = 0

        self.jobLock.acquire()

        for job in self.job_status:
            if self.job_status[job] == 'running':
                jobs_left += 1

        self.jobLock.release()

        return jobs_left

    # synchronized method to write output
    # to a log file to avoid any overwrites
    def output_log(self, message):

        self.logger.log(level=logging.INFO, msg=message)



    # method used to signal that a
    # server shutdown is requested
    def shutdown(self):
        self.shutdown = True
