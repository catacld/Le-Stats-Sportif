import os
import json
import pandas


class Record:

    def __init__(self, question, dataValue, locationDesc):
        self.question = question
        self.dataValue = dataValue
        self.locationDesc = locationDesc


# singleton wrapper class used to share the records between this class and task_runner
class RecordsWrapper:
    _self = None  # Store the single instance

    def __new__(cls):
        if cls._self is None:
            cls._self = super().__new__(cls)
            cls._self.records = []
        return cls._self

    def setRecords(cls, records):
        cls._self.records = records


class DataIngestor:

    def __init__(self, csv_path: str):
        # TODO: Read csv from csv_path

        data = pandas.read_csv(csv_path, usecols=['Question', 'Data_Value', 'LocationDesc'])

        records = [Record(row['Question'], row['Data_Value'], row['LocationDesc']) for index, row in data.iterrows()]

        # for item in records:
        #     print(f"Question: {item.question}, Data Value: {item.dataValue}, locationDesc: {item.locationDesc}")

        # init the singleton wrapper class
        recordsWrapper = RecordsWrapper()
        # add the records
        recordsWrapper.setRecords(records)

        # for item in recordsWrapper.records:
        #     print(f"Question: {item.question}, Data Value: {item.dataValue}, locationDesc: {item.locationDesc}")

        self.questions_best_is_min = [
            'Percent of adults aged 18 years and older who have an overweight classification',
            'Percent of adults aged 18 years and older who have obesity',
            'Percent of adults who engage in no leisure-time physical activity',
            'Percent of adults who report consuming fruit less than one time daily',
            'Percent of adults who report consuming vegetables less than one time daily'
        ]

        self.questions_best_is_max = [
            'Percent of adults who achieve at least 150 minutes a week of moderate-intensity aerobic physical activity or 75 minutes a week of vigorous-intensity aerobic activity (or an equivalent combination)',
            'Percent of adults who achieve at least 150 minutes a week of moderate-intensity aerobic physical activity or 75 minutes a week of vigorous-intensity aerobic physical activity and engage in muscle-strengthening activities on 2 or more days a week',
            'Percent of adults who achieve at least 300 minutes a week of moderate-intensity aerobic physical activity or 150 minutes a week of vigorous-intensity aerobic activity (or an equivalent combination)',
            'Percent of adults who engage in muscle-strengthening activities on 2 or more days a week',
        ]
