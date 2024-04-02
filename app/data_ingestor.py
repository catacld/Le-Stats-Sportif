import os
import json
import pandas

from app.database import Database


class Record:

    def __init__(self, question, dataValue, locationDesc, stratificationCategory, stratification):
        self.question = question
        self.dataValue = dataValue
        self.locationDesc = locationDesc
        self.stratificationCategory = stratificationCategory
        self.stratification = stratification


class DataIngestor:

    def __init__(self, csv_path: str):
        # TODO: Read csv from csv_path

        data = pandas.read_csv(csv_path, usecols=['Question', 'Data_Value', 'LocationDesc',
                                                  'StratificationCategory1', 'Stratification1'])

        records = [Record(row['Question'], row['Data_Value'], row['LocationDesc'], row['StratificationCategory1'],
                          row['Stratification1']) for index, row in data.iterrows()]

        # init the singleton wrapper class
        database = Database()
        # add the records
        database.setRecords(records)

        # self.questions_best_is_min = [
        #     'Percent of adults aged 18 years and older who have an overweight classification',
        #     'Percent of adults aged 18 years and older who have obesity',
        #     'Percent of adults who engage in no leisure-time physical activity',
        #     'Percent of adults who report consuming fruit less than one time daily',
        #     'Percent of adults who report consuming vegetables less than one time daily'
        # ]
        #
        # self.questions_best_is_max = [
        #     'Percent of adults who achieve at least 150 minutes a week of moderate-intensity aerobic physical activity or 75 minutes a week of vigorous-intensity aerobic activity (or an equivalent combination)',
        #     'Percent of adults who achieve at least 150 minutes a week of moderate-intensity aerobic physical activity or 75 minutes a week of vigorous-intensity aerobic physical activity and engage in muscle-strengthening activities on 2 or more days a week',
        #     'Percent of adults who achieve at least 300 minutes a week of moderate-intensity aerobic physical activity or 150 minutes a week of vigorous-intensity aerobic activity (or an equivalent combination)',
        #     'Percent of adults who engage in muscle-strengthening activities on 2 or more days a week',
        # ]
