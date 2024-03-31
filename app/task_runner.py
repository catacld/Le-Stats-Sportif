import os
from queue import Queue
from threading import Thread, Event
import time
import json

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
                    print('statesMeanRequest')
                elif job.requestType == 'stateMeanRequest':
                    #print('stateMeanRequest')

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

                    print('\nGIVEN STATE: ', state)

                    for item in recordsWrapper.records:
                        if item.locationDesc == state:
                            print(f"locationDesc: {item.locationDesc}")

                    print('\nstatesDiffFromMeanRequestPRINT')

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
