import os
import threading
from queue import Queue
from threading import Thread
import json
from itertools import islice

from .database import Database


class ThreadPool:
    def __init__(self):
        # You must implement a ThreadPool of TaskRunners
        # Your ThreadPool should check if an environment variable TP_NUM_OF_THREADS is defined
        # If the env var is defined, that is the number of threads to be used by the thread pool
        # Otherwise, you are to use what the hardware concurrency allows
        # You are free to write your implementation as you see fit, but
        # You must NOT:
        #   * create more threads than the hardware concurrency allows
        #   * recreate threads for each task

        # check if env var is defined else use cpu_count
        if os.getenv('TP_NUM_OF_THREADS') is not None:
            self.numThreads = int(os.getenv('TP_NUM_OF_THREADS'))
        else:
            self.numThreads = os.cpu_count()

        # all the threads from the pool
        self.workers = []
        # all the tasks
        self.tasks = Queue()

        self.lock = threading.Lock()

        self.database = Database()

        for i in range(self.numThreads):
            # create a new thread and share the tasks list
            worker = TaskRunner(self.tasks, self.lock)
            # start the new thread
            worker.start()
            # add the new thread to the list
            self.workers.append(worker)

        if self.tasks.empty() and self.database.shutdown is True:
            for worker in self.workers:
                worker.join()

        pass


class TaskRunner(Thread):
    def __init__(self, tasks, lock):

        Thread.__init__(self)
        self.tasks = tasks
        self.database = Database()
        self.lock = lock
        pass

    def run(self):
        while True and self.database.shutdown is False:

            # wrapper used to access the 'database'
            database = Database()

            if not self.tasks.empty():
                # no locks needed since queues are already synchronized
                job = self.tasks.get()

                # assign an id to the current job and set the status as running
                id = 'job_id_' + str(job.jobId)
                database.setJobStatus(id, 'running')

                # extract the request' question
                question = job.requestQuestion

                # create the output path of the file
                current_dir = os.getcwd()
                output_file = f"{id}.json"
                output_path = current_dir + "/results/" + output_file

                os.makedirs(os.path.dirname(output_path), exist_ok=True)

                if job.requestType == 'statesMeanRequest':

                    averageForEachState = {}
                    sumForEachState = {}
                    numberOfValuesForEachState = {}

                    for item in database.records:
                        if item.question == question:
                            sumForEachState[item.locationDesc] = (sumForEachState.get(item.locationDesc, 0)
                                                                  + item.dataValue)
                            numberOfValuesForEachState[item.locationDesc] = (
                                    numberOfValuesForEachState.get(item.locationDesc, 0)
                                    + 1)

                    for key in sumForEachState:
                        averageForEachState[key] = sumForEachState[key] / numberOfValuesForEachState[key]

                    sortedAverages = dict(
                        sorted(averageForEachState.items(), key=lambda item: item[1]))

                    with open(output_path, 'w+') as f:
                        f.write(json.dumps(sortedAverages))

                    database.setJobStatus(id, 'done')




                elif job.requestType == 'stateMeanRequest':

                    state = job.state

                    sum = 0
                    num = 0

                    for item in database.records:
                        if item.locationDesc == state and item.question == question:
                            sum += item.dataValue
                            num += 1

                    average = sum / num
                    answer = {state: average}

                    # write the output of the request
                    with open(output_path, 'w+') as f:
                        f.write(json.dumps(answer))

                    database.setJobStatus(id, 'done')

                elif job.requestType == 'statesDiffFromMeanRequest':
                    state = job.state

                    globalMeanValue = 0
                    numberOfGlobalValues = 0
                    stateMeanValue = 0
                    numberOfStateValues = 0

                    for item in database.records:
                        if item.question == question:
                            if item.locationDesc == state:
                                stateMeanValue += item.dataValue
                                numberOfStateValues += 1
                            globalMeanValue += item.dataValue
                            numberOfGlobalValues += 1

                    result = (globalMeanValue / numberOfGlobalValues) - (stateMeanValue / numberOfStateValues)

                    answer = {state: result}

                    # write the output of the request
                    with open(output_path, 'w+') as f:
                        f.write(json.dumps(answer))

                    database.setJobStatus(id, 'done')



                elif job.requestType == 'best5Request':
                    averageForEachState = {}
                    sumForEachState = {}
                    numberOfValuesForEachState = {}

                    # question where greater is better
                    sortDescending = ['Percent of adults who achieve at least 300 minutes a week of '
                                      'moderate-intensity aerobic physical activity or 150 minutes a week of '
                                      'vigorous-intensity aerobic activity (or an equivalent combination)',
                                      'Percent of adults who achieve at least 150 minutes a week of '
                                      'moderate-intensity aerobic physical activity or 75 minutes a week of '
                                      'vigorous-intensity aerobic physical activity and engage in '
                                      'muscle-strengthening activities on 2 or more days a week',
                                      'Percent of adults who achieve at least 150 minutes a week of '
                                      'moderate-intensity aerobic physical activity or 75 minutes a week of '
                                      'vigorous-intensity aerobic activity (or an equivalent combination)',
                                      'Percent of adults who engage in muscle-strengthening activities on 2 or more '
                                      'days a week']

                    for item in database.records:
                        if item.question == question:
                            sumForEachState[item.locationDesc] = (sumForEachState.get(item.locationDesc, 0)
                                                                  + item.dataValue)
                            numberOfValuesForEachState[item.locationDesc] = (
                                    numberOfValuesForEachState.get(item.locationDesc, 0)
                                    + 1)

                    for key in sumForEachState:
                        averageForEachState[key] = sumForEachState[key] / numberOfValuesForEachState[key]

                    if question in sortDescending:
                        sortedAverages = dict(
                            sorted(averageForEachState.items(), key=lambda item: item[1], reverse=True))
                    else:
                        sortedAverages = dict(
                            sorted(averageForEachState.items(), key=lambda item: item[1]))

                    sortedAverages = dict(islice(sortedAverages.items(), 5))

                    with open(output_path, 'w+') as f:
                        f.write(json.dumps(sortedAverages))

                    database.setJobStatus(id, 'done')


                elif job.requestType == 'worst5Request':
                    averageForEachState = {}
                    sumForEachState = {}
                    numberOfValuesForEachState = {}

                    # question where greater is better
                    sortAscending = ['Percent of adults who achieve at least 300 minutes a week of '
                                     'moderate-intensity aerobic physical activity or 150 minutes a week of '
                                     'vigorous-intensity aerobic activity (or an equivalent combination)',
                                     'Percent of adults who achieve at least 150 minutes a week of '
                                     'moderate-intensity aerobic physical activity or 75 minutes a week of '
                                     'vigorous-intensity aerobic physical activity and engage in '
                                     'muscle-strengthening activities on 2 or more days a week',
                                     'Percent of adults who achieve at least 150 minutes a week of '
                                     'moderate-intensity aerobic physical activity or 75 minutes a week of '
                                     'vigorous-intensity aerobic activity (or an equivalent combination)',
                                     'Percent of adults who engage in muscle-strengthening activities on 2 or more '
                                     'days a week']

                    for item in database.records:
                        if item.question == question:
                            sumForEachState[item.locationDesc] = (sumForEachState.get(item.locationDesc, 0)
                                                                  + item.dataValue)
                            numberOfValuesForEachState[item.locationDesc] = (
                                    numberOfValuesForEachState.get(item.locationDesc, 0)
                                    + 1)

                    for key in sumForEachState:
                        averageForEachState[key] = sumForEachState[key] / numberOfValuesForEachState[key]

                    if question in sortAscending:
                        sortedAverages = dict(
                            sorted(averageForEachState.items(), key=lambda item: item[1]))
                    else:
                        sortedAverages = dict(
                            sorted(averageForEachState.items(), key=lambda item: item[1], reverse=True))

                    sortedAverages = dict(islice(sortedAverages.items(), 5))

                    with open(output_path, 'w+') as f:
                        f.write(json.dumps(sortedAverages))

                    database.setJobStatus(id, 'done')

                elif job.requestType == 'diffFromMeanRequest':
                    diffForEachState = {}
                    sumForEachState = {}
                    numberOfValuesForEachState = {}

                    globalSum = 0
                    numberOfGlobalValues = 0

                    for item in database.records:
                        if item.question == question:
                            sumForEachState[item.locationDesc] = (sumForEachState.get(item.locationDesc, 0)
                                                                  + item.dataValue)
                            numberOfValuesForEachState[item.locationDesc] = (
                                    numberOfValuesForEachState.get(item.locationDesc, 0)
                                    + 1)
                            globalSum += item.dataValue
                            numberOfGlobalValues += 1

                    for key in sumForEachState:
                        diffForEachState[key] = globalSum / numberOfGlobalValues - sumForEachState[key] / \
                                                numberOfValuesForEachState[key]

                    with open(output_path, 'w+') as f:
                        f.write(json.dumps(diffForEachState))

                    database.setJobStatus(id, 'done')

                elif job.requestType == 'stateMeanByCategoryRequest':
                    state = job.state

                    sumForEachSegment = {}
                    numberOfValuesForEachSegment = {}
                    averageForEachSegment = {}

                    for item in database.records:
                        if item.locationDesc == state and item.question == question:

                            # check that there are values available
                            if type(item.stratificationCategory) is not float and type(
                                    item.stratification) is not float:
                                key = '(\'' + item.stratificationCategory + '\', \'' + item.stratification + '\')'

                                sumForEachSegment[key] = sumForEachSegment.get(key, 0) + item.dataValue
                                numberOfValuesForEachSegment[key] = numberOfValuesForEachSegment.get(key, 0) + 1

                    for key in sumForEachSegment:
                        averageForEachSegment[key] = sumForEachSegment[key] / numberOfValuesForEachSegment[key]

                    answer = {state: averageForEachSegment}

                    # write the output of the request
                    with open(output_path, 'w+') as f:
                        f.write(json.dumps(answer))

                    database.setJobStatus(id, 'done')


                elif job.requestType == 'meanByCategoryRequest':

                    sumForEachSegment = {}
                    numberOfValuesForEachSegment = {}
                    averageForEachSegment = {}

                    for item in database.records:
                        if item.question == question:

                            # check that there are values available
                            if type(item.stratificationCategory) is not float and type(
                                    item.stratification) is not float:
                                key = ('(\'' + item.locationDesc + '\', \'' + item.stratificationCategory +
                                       '\', \'' + item.stratification + '\')')

                                sumForEachSegment[key] = sumForEachSegment.get(key, 0) + item.dataValue
                                numberOfValuesForEachSegment[key] = numberOfValuesForEachSegment.get(key, 0) + 1

                    for key in sumForEachSegment:
                        averageForEachSegment[key] = sumForEachSegment[key] / numberOfValuesForEachSegment[key]

                    # answer = {state: averageForEachSegment}

                    # write the output of the request
                    with open(output_path, 'w+') as f:
                        f.write(json.dumps(averageForEachSegment))

                    database.setJobStatus(id, 'done')



                elif job.requestType == 'globalMeanRequest':

                    sum = 0
                    num = 0
                    for item in database.records:
                        if item.question == question:
                            sum += item.dataValue
                            num += 1

                    average = sum / num
                    answer = {"global_mean": average}

                    # write the output of the request
                    with open(output_path, 'w+') as f:
                        f.write(json.dumps(answer))

                    database.setJobStatus(id, 'done')

                # mark task as done
                self.tasks.task_done()
