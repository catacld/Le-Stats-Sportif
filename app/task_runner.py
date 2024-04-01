import os
from queue import Queue
from threading import Thread, Event
import time
import json
from itertools import islice

from .data_ingestor import RecordsWrapper


class JobsWrapper:
    _self = None  # Store the single instance

    def __new__(cls):
        if cls._self is None:
            cls._self = super().__new__(cls)
            cls._self.finishedJobs = {}
        return cls._self

    def get_jobs(cls):
        return cls._self.finishedJobs


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

        for i in range(self.numThreads):
            # create a new thread and share the tasks list
            worker = TaskRunner(self.tasks)
            # start the new thread
            worker.start()
            # add the new thread to the list
            self.workers.append(worker)

        # print(self.numThreads)
        pass


class TaskRunner(Thread):
    def __init__(self, tasks):
        # TODO: init necessary data structures
        Thread.__init__(self)
        self.tasks = tasks
        pass

    def run(self):
        while True:
            # TODO
            # Get pending job
            # Execute the job and save the result to disk
            # Repeat until graceful_shutdown

            # wrapper used to access the 'database'
            recordsWrapper = RecordsWrapper()
            jobsWrapper = JobsWrapper()

            if not self.tasks.empty():
                job = self.tasks.get()

                # create the output path of the file
                current_dir = os.getcwd()
                output_file = f"job_id_{job.requestId}"
                output_path = current_dir + "/results/" + output_file

                os.makedirs(os.path.dirname(output_path), exist_ok=True)

                if job.requestType == 'statesMeanRequest':

                    averageForEachState = {}
                    sumForEachState = {}
                    numberOfValuesForEachState = {}

                    question = job.requestQuestion
                    id = job.requestId



                    for item in recordsWrapper.records:
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

                    jobsWrapper.finishedJobs[id] = sortedAverages

                    with open(output_path, 'w+') as f:
                        f.write(json.dumps(sortedAverages))





                elif job.requestType == 'stateMeanRequest':
                    # print('stateMeanRequest')

                    state = job.state
                    question = job.requestQuestion
                    id = job.requestId

                    sum = 0;
                    num = 0;

                    for item in recordsWrapper.records:
                        if item.locationDesc == state and item.question == question:
                            sum += item.dataValue
                            num += 1

                    average = sum / num
                    answer = {state: average}

                    # add the result of the job
                    jobsWrapper.finishedJobs[id] = answer

                    # write the output of the request
                    with open(output_path, 'w+') as f:
                        f.write(json.dumps(answer))

                elif job.requestType == 'statesDiffFromMeanRequest':
                    state = job.state
                    question = job.requestQuestion
                    id = job.requestId

                    globalMeanValue = 0
                    numberOfGlobalValues = 0
                    stateMeanValue = 0
                    numberOfStateValues = 0

                    for item in recordsWrapper.records:
                        if item.question == question:
                            if item.locationDesc == state:
                                stateMeanValue += item.dataValue
                                numberOfStateValues += 1
                            globalMeanValue += item.dataValue
                            numberOfGlobalValues += 1

                    result = (globalMeanValue / numberOfGlobalValues) - (stateMeanValue / numberOfStateValues)

                    answer = {state: result}

                    # add the result of the job
                    jobsWrapper.finishedJobs[id] = answer

                    # write the output of the request
                    with open(output_path, 'w+') as f:
                        f.write(json.dumps(answer))



                elif job.requestType == 'best5Request':
                    averageForEachState = {}
                    sumForEachState = {}
                    numberOfValuesForEachState = {}

                    question = job.requestQuestion
                    id = job.requestId

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

                    for item in recordsWrapper.records:
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

                    jobsWrapper.finishedJobs[id] = sortedAverages

                    with open(output_path, 'w+') as f:
                        f.write(json.dumps(sortedAverages))


                elif job.requestType == 'worst5Request':
                    averageForEachState = {}
                    sumForEachState = {}
                    numberOfValuesForEachState = {}

                    question = job.requestQuestion
                    id = job.requestId

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

                    for item in recordsWrapper.records:
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

                    jobsWrapper.finishedJobs[id] = sortedAverages

                    with open(output_path, 'w+') as f:
                        f.write(json.dumps(sortedAverages))
                elif job.requestType == 'diffFromMeanRequest':
                    diffForEachState = {}
                    sumForEachState = {}
                    numberOfValuesForEachState = {}

                    globalSum = 0
                    numberOfGlobalValues = 0

                    question = job.requestQuestion
                    id = job.requestId

                    for item in recordsWrapper.records:
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

                    jobsWrapper.finishedJobs[id] = diffForEachState

                    with open(output_path, 'w+') as f:
                        f.write(json.dumps(diffForEachState))

                elif job.requestType == 'stateMeanByCategoryRequest':
                    state = job.state
                    question = job.requestQuestion
                    id = job.requestId

                    sumForEachSegment = {}
                    numberOfValuesForEachSegment = {}
                    averageForEachSegment = {}

                    for item in recordsWrapper.records:
                        if item.locationDesc == state and item.question == question:

                            # check that there are values available
                            if type(item.stratificationCategory) is not float and type(
                                    item.stratification) is not float:
                                key = '(\'' + item.stratificationCategory + '\', \'' + item.stratification + '\')'

                                sumForEachSegment[key] = sumForEachSegment.get(key, 0) + item.dataValue
                                numberOfValuesForEachSegment[key] = numberOfValuesForEachSegment.get(key, 0) + 1

                            # print('\nTHE KEY IS: ', key)

                    for key in sumForEachSegment:
                        averageForEachSegment[key] = sumForEachSegment[key] / numberOfValuesForEachSegment[key]

                    answer = {state: averageForEachSegment}

                    # add the result of the job
                    jobsWrapper.finishedJobs[id] = answer

                    # write the output of the request
                    with open(output_path, 'w+') as f:
                        f.write(json.dumps(answer))


                elif job.requestType == 'meanByCategoryRequest':
                    question = job.requestQuestion
                    id = job.requestId

                    sumForEachSegment = {}
                    numberOfValuesForEachSegment = {}
                    averageForEachSegment = {}

                    for item in recordsWrapper.records:
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

                    # add the result of the job
                    jobsWrapper.finishedJobs[id] = averageForEachSegment

                    # write the output of the request
                    with open(output_path, 'w+') as f:
                        f.write(json.dumps(averageForEachSegment))



                elif job.requestType == 'globalMeanRequest':
                    # print('stateMeanRequest')

                    state = job.state
                    question = job.requestQuestion
                    id = job.requestId

                    sum = 0;
                    num = 0;
                    for item in recordsWrapper.records:
                        if item.question == question:
                            sum += item.dataValue
                            num += 1

                    average = sum / num
                    answer = {"global_mean": average}

                    # add the result of the job
                    jobsWrapper.finishedJobs[id] = answer

                    # write the output of the request
                    with open(output_path, 'w+') as f:
                        f.write(json.dumps(answer))

                # mark task as done
                self.tasks.task_done()
