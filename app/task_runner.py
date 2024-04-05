import os
import threading
from threading import Thread
import json
from itertools import islice

from .database import Database


class ThreadPool:
    def __init__(self):
        if os.getenv('TP_NUM_OF_THREADS') is not None:
            self.numThreads = int(os.getenv('TP_NUM_OF_THREADS'))
        else:
            self.numThreads = os.cpu_count()

        # all the threads from the pool
        self.workers = []

        self.lock = threading.Lock()

        self.database = Database()

        for i in range(self.numThreads):
            # create a new thread and share the tasks list
            worker = TaskRunner(self.lock)
            # start the new thread
            worker.start()
            # add the new thread to the list
            self.workers.append(worker)

        # if a shutdown has been requested and there are
        # no tasks left to process, end the threads
        if self.database.tasks.empty() and self.database.shutdown is True:
            for worker in self.workers:
                worker.join()

        pass


class TaskRunner(Thread):
    def __init__(self, lock):
        Thread.__init__(self)
        self.database = Database()
        self.lock = lock
        pass

    def run(self):
        while True and self.database.shutdown is False:

            # wrapper used to access the 'database'
            database = Database()

            if not self.database.tasks.empty():
                # no locks needed since queues are already synchronized
                job = self.database.tasks.get()

                # assign an id to the current job and set the status as running
                id = 'job_id_' + str(job.jobId)
                database.set_job_status(id, 'running')

                # extract the request's question
                question = job.requestQuestion

                # create the output path of the file
                current_dir = os.getcwd()
                output_file = f"{id}.json"
                output_path = current_dir + "/results/" + output_file

                os.makedirs(os.path.dirname(output_path), exist_ok=True)

                if job.requestType == 'statesMeanRequest':
                    # dictionaries used to store the values
                    average_for_each_state = {}
                    sum_for_each_state = {}
                    number_of_values_for_each_state = {}

                    # iterate over the values from the given csv
                    for item in database.records:
                        if item.question == question:
                            sum_for_each_state[item.locationDesc] = (sum_for_each_state.get(item.locationDesc, 0)
                                                                     + item.dataValue)
                            number_of_values_for_each_state[item.locationDesc] = (
                                    number_of_values_for_each_state.get(item.locationDesc, 0)
                                    + 1)

                    for key in sum_for_each_state:
                        average_for_each_state[key] = sum_for_each_state[key] / number_of_values_for_each_state[key]

                    sorted_averages = dict(
                        sorted(average_for_each_state.items(), key=lambda item: item[1]))

                    # write the result of the request inside a file
                    with open(output_path, 'w+') as f:
                        f.write(json.dumps(sorted_averages))

                    database.set_job_status(id, 'done')

                elif job.requestType == 'stateMeanRequest':

                    state = job.state

                    sum = 0
                    num = 0

                    # iterate over the values from the given csv file
                    for item in database.records:
                        if item.locationDesc == state and item.question == question:
                            sum += item.dataValue
                            num += 1

                    average = sum / num
                    answer = {state: average}

                    # write the result of the request
                    with open(output_path, 'w+') as f:
                        f.write(json.dumps(answer))

                    database.set_job_status(id, 'done')

                elif job.requestType == 'statesDiffFromMeanRequest':
                    state = job.state

                    # variables needed to calculate the values
                    global_mean_value = 0
                    number_of_global_values = 0
                    state_mean_value = 0
                    number_of_state_values = 0

                    # iterate over the values from the csv
                    for item in database.records:
                        if item.question == question:
                            if item.locationDesc == state:
                                state_mean_value += item.dataValue
                                number_of_state_values += 1
                            global_mean_value += item.dataValue
                            number_of_global_values += 1

                    result = (global_mean_value / number_of_global_values) - (state_mean_value / number_of_state_values)

                    answer = {state: result}

                    # write the output of the request
                    with open(output_path, 'w+') as f:
                        f.write(json.dumps(answer))

                    database.set_job_status(id, 'done')

                elif job.requestType == 'best5Request':
                    average_for_each_state = {}
                    sum_for_each_state = {}
                    number_of_values_for_each_state = {}

                    # questions where greater is better
                    sort_descending = ['Percent of adults who achieve at least 300 minutes a week of '
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

                    # iterate over the values from the csv
                    for item in database.records:
                        if item.question == question:
                            sum_for_each_state[item.locationDesc] = (sum_for_each_state.get(item.locationDesc, 0)
                                                                     + item.dataValue)
                            number_of_values_for_each_state[item.locationDesc] = (
                                    number_of_values_for_each_state.get(item.locationDesc, 0)
                                    + 1)

                    for key in sum_for_each_state:
                        average_for_each_state[key] = sum_for_each_state[key] / number_of_values_for_each_state[key]

                    # sort either descending or ascending depending on the question
                    # to get the best 5
                    if question in sort_descending:
                        sorted_averages = dict(
                            sorted(average_for_each_state.items(), key=lambda item: item[1], reverse=True))
                    else:
                        sorted_averages = dict(
                            sorted(average_for_each_state.items(), key=lambda item: item[1]))

                    sorted_averages = dict(islice(sorted_averages.items(), 5))

                    # write the output of the request to a file
                    with open(output_path, 'w+') as f:
                        f.write(json.dumps(sorted_averages))

                    database.set_job_status(id, 'done')

                elif job.requestType == 'worst5Request':
                    average_for_each_state = {}
                    sum_for_each_state = {}
                    number_of_values_for_each_state = {}

                    # questions where greater is better
                    sort_ascending = ['Percent of adults who achieve at least 300 minutes a week of '
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

                    # iterate over the values from the csv
                    for item in database.records:
                        if item.question == question:
                            sum_for_each_state[item.locationDesc] = (sum_for_each_state.get(item.locationDesc, 0)
                                                                     + item.dataValue)
                            number_of_values_for_each_state[item.locationDesc] = (
                                    number_of_values_for_each_state.get(item.locationDesc, 0)
                                    + 1)

                    for key in sum_for_each_state:
                        average_for_each_state[key] = sum_for_each_state[key] / number_of_values_for_each_state[key]

                    # sort depending on the given question
                    if question in sort_ascending:
                        sorted_averages = dict(
                            sorted(average_for_each_state.items(), key=lambda item: item[1]))
                    else:
                        sorted_averages = dict(
                            sorted(average_for_each_state.items(), key=lambda item: item[1], reverse=True))

                    sorted_averages = dict(islice(sorted_averages.items(), 5))

                    # write the result of the request inside a file
                    with open(output_path, 'w+') as f:
                        f.write(json.dumps(sorted_averages))

                    database.set_job_status(id, 'done')

                elif job.requestType == 'diffFromMeanRequest':
                    # values needed to calculate the request
                    diff_for_each_state = {}
                    sum_for_each_state = {}
                    number_of_values_for_each_state = {}

                    global_sum = 0
                    number_of_global_values = 0

                    # iterate over the values from the csv
                    for item in database.records:
                        if item.question == question:
                            sum_for_each_state[item.locationDesc] = (sum_for_each_state.get(item.locationDesc, 0)
                                                                     + item.dataValue)
                            number_of_values_for_each_state[item.locationDesc] = (
                                    number_of_values_for_each_state.get(item.locationDesc, 0)
                                    + 1)
                            global_sum += item.dataValue
                            number_of_global_values += 1

                    for key in sum_for_each_state:
                        diff_for_each_state[key] = global_sum / number_of_global_values - sum_for_each_state[key] / \
                                                   number_of_values_for_each_state[key]

                    # write the output to a file
                    with open(output_path, 'w+') as f:
                        f.write(json.dumps(diff_for_each_state))

                    database.set_job_status(id, 'done')

                elif job.requestType == 'stateMeanByCategoryRequest':
                    state = job.state

                    # variables needed to calculate the requests
                    sum_for_each_segment = {}
                    number_of_values_for_each_segment = {}
                    average_for_each_segment = {}

                    for item in database.records:
                        if item.locationDesc == state and item.question == question:

                            # check that there are values available
                            if type(item.stratificationCategory) is not float and type(
                                    item.stratification) is not float:
                                key = '(\'' + item.stratificationCategory + '\', \'' + item.stratification + '\')'

                                sum_for_each_segment[key] = sum_for_each_segment.get(key, 0) + item.dataValue
                                number_of_values_for_each_segment[key] = number_of_values_for_each_segment.get(key,
                                                                                                               0) + 1

                    for key in sum_for_each_segment:
                        average_for_each_segment[key] = sum_for_each_segment[key] / number_of_values_for_each_segment[
                            key]

                    answer = {state: average_for_each_segment}

                    # write the output of the request
                    with open(output_path, 'w+') as f:
                        f.write(json.dumps(answer))

                    database.set_job_status(id, 'done')

                elif job.requestType == 'meanByCategoryRequest':
                    # variables needed to calculate the values
                    sum_for_each_segment = {}
                    number_of_values_for_each_segment = {}
                    average_for_each_segment = {}

                    # iterate over the values from the csv
                    for item in database.records:
                        if item.question == question:

                            # check that there are values available
                            if type(item.stratificationCategory) is not float and type(
                                    item.stratification) is not float:
                                key = ('(\'' + item.locationDesc + '\', \'' + item.stratificationCategory +
                                       '\', \'' + item.stratification + '\')')

                                sum_for_each_segment[key] = sum_for_each_segment.get(key, 0) + item.dataValue
                                number_of_values_for_each_segment[key] = number_of_values_for_each_segment.get(key,
                                                                                                               0) + 1

                    for key in sum_for_each_segment:
                        average_for_each_segment[key] = sum_for_each_segment[key] / number_of_values_for_each_segment[
                            key]

                    # write the output of the request
                    with open(output_path, 'w+') as f:
                        f.write(json.dumps(average_for_each_segment))

                    database.set_job_status(id, 'done')

                elif job.requestType == 'globalMeanRequest':

                    sum = 0
                    num = 0

                    # iterate over the values from the csv
                    for item in database.records:
                        if item.question == question:
                            sum += item.dataValue
                            num += 1

                    average = sum / num
                    answer = {"global_mean": average}

                    # write the output of the request
                    with open(output_path, 'w+') as f:
                        f.write(json.dumps(answer))

                    database.set_job_status(id, 'done')

                # mark task as done
                self.database.tasks.task_done()
