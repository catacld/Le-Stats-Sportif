import json
import os
import threading
import logging
import time


class Database:
    _self = None

    shutdown = False

    def __new__(cls):
        if cls._self is None:
            cls._self = super().__new__(cls)
            cls._self.records = []
            cls._self.jobStatus = {}
            cls._self.shutdown = False
            cls._self.jobId = 1

            cls._self.idLock = threading.Lock()
            cls._self.logLock = threading.Lock()
            cls._self.jobLock = threading.Lock()


            # cls._self.logger = logging.getLogger(__name__)
            # logging.basicConfig(filename='webserver.log', level=logging.INFO)
            #
            # formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            # formatter.converter = time.gmtime
            #
            # ch = logging.StreamHandler()
            # ch.setFormatter(formatter)
            #
            # cls._self.logger.addHandler(ch)

        return cls._self

    def setRecords(cls, records):
        cls._self.records = records

    # helper method to make sure that ids assignation
    # will be synchronized
    def assignJobId(self):
        self.idLock.acquire()

        self.jobId += 1

        self.idLock.release()

        return self.jobId



    # define a function for each interaction with the dictionary to
    # ensure that only 1 thread will acces the dictionary at any
    # given point in time to avoid any wrong reads


    # synchronized methods to access the dictionary
    # to make sure the value that is read
    # will always be correct

    def setJobStatus(self, jobId, status):


        self.jobLock.acquire()


        self.jobStatus[jobId] = status

        self.jobLock.release()

    def getJobStatus(self, jobId):
        self.jobLock.acquire()

        if jobId not in self.jobStatus:
            self.jobLock.release()
            return 'invalid id'

        status = self.jobStatus[jobId]

        self.jobLock.release()

        return status

    def getJobsLeft(self):


        jobsLeft = 0;

        self.jobLock.acquire()

        for job in self.jobStatus:
            if self.jobStatus[job] == 'running':
                jobsLeft += 1


        self.jobLock.release()


        return jobsLeft



    def shutdown(cls):
        cls._self.shutdown = True

