import threading
import logging
import time
from logging.handlers import RotatingFileHandler


class Database:
    _self = None

    def __new__(cls):
        if cls._self is None:
            cls._self = super().__new__(cls)
            cls._self.records = []
            cls._self.jobStatus = {}
            cls._self.shutdown = False
            cls._self.jobId = 1

            # locks needed for the synchronized methods
            cls._self.idLock = threading.Lock()
            cls._self.logLock = threading.Lock()
            cls._self.jobLock = threading.Lock()
            cls._self.logLock = threading.Lock()

            # logger setup
            cls._self.logger = logging.getLogger(__name__)
            cls._self.logger.setLevel(logging.INFO)

            handler = RotatingFileHandler('webserver.log', mode='a', maxBytes=100000, backupCount=10)
            formatter = logging.Formatter('%(asctime)s %(levelname)-8s %(name)-15s:%(lineno)4s: %(message)-80s',
                                          datefmt='%Y-%m-%d %H:%M:%S')
            formatter.converter = time.gmtime
            handler.setFormatter(formatter)

            cls._self.logger.addHandler(handler)

        return cls._self

    # helper method used to set the records
    # with the values read from the csv
    def setRecords(cls, records):
        cls._self.records = records

    # synchronized method to make sure that id assignation
    # will be synchronized
    def assignJobId(self):
        self.idLock.acquire()

        self.jobId += 1

        self.idLock.release()

        return self.jobId

    # synchronized methods to access the dictionary
    # where each job's status is stored
    # to make sure the value that is read
    # will always be correct

    def setJobStatus(self, jobId, status):

        self.jobLock.acquire()

        self.jobStatus[jobId] = status

        self.jobLock.release()

    def getJobStatus(self, job_id):
        self.jobLock.acquire()

        if job_id not in self.jobStatus:
            self.jobLock.release()
            return 'invalid id'

        status = self.jobStatus[job_id]

        self.jobLock.release()

        return status

    def getJobsLeft(self):

        jobs_left = 0

        self.jobLock.acquire()

        for job in self.jobStatus:
            if self.jobStatus[job] == 'running':
                jobs_left += 1

        self.jobLock.release()

        return jobs_left

    # synchronized method to write output
    # to a log file to avoid any overwrites
    def outputLog(self, message):

        self.logLock.acquire()

        self.logger.log(level=logging.INFO, msg=message)

        self.logLock.release()

    # method used to signal that a
    # server shutdown is requested
    def shutdown(cls):
        cls._self.shutdown = True
