import json
import os
import threading


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
            cls._self.lock = threading.Lock()
        return cls._self

    def setRecords(cls, records):
        cls._self.records = records

    # helper method to make sure that ids assignation
    # will be synchronized
    def assignJobId(self):
        self.lock.acquire()

        self.jobId += 1

        self.lock.release()

        return self.jobId

    def shutdown(cls):
        cls._self.shutdown = True
