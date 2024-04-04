import pandas

from app.database import Database


# class used to save the records from the given csv file
class Record:

    def __init__(self, question, data_value, location_desc, stratification_category, stratification):
        self.question = question
        self.dataValue = data_value
        self.locationDesc = location_desc
        self.stratificationCategory = stratification_category
        self.stratification = stratification


class DataIngestor:

    def __init__(self, csv_path: str):
        # parse the csv file
        data = pandas.read_csv(csv_path, usecols=['Question', 'Data_Value', 'LocationDesc',
                                                  'StratificationCategory1', 'Stratification1'])

        # save the data from the csv to a dictionary
        records = [Record(row['Question'], row['Data_Value'], row['LocationDesc'], row['StratificationCategory1'],
                          row['Stratification1']) for index, row in data.iterrows()]

        # init the singleton wrapper class
        database = Database()
        # add the records
        database.setRecords(records)
