


class Database:
    _self = None

    def __new__(cls):
        if cls._self is None:
            cls._self = super().__new__(cls)
            cls._self.records = []
            cls._self.jobStatus = {}
        return cls._self

    def setRecords(cls, records):
        cls._self.records = records