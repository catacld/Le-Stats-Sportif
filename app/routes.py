from app import webserver
from flask import request, jsonify

from .task_runner import ThreadPool, JobsWrapper

import os
import json

assigned_job_id = 1
threadPool = ThreadPool()


class Job:

    def __init__(self, requestId, requestType, requestQuestion, state = None):
        self.requestId = requestId
        self.requestType = requestType
        self.requestQuestion = requestQuestion
        # used for ordering in best5 and worst5
        self.state = state


# Example endpoint definition
@webserver.route('/api/post_endpoint', methods=['POST'])
def post_endpoint():
    if request.method == 'POST':
        # Assuming the request contains JSON data
        data = request.json
        print(f"got data in post {data}")

        # Process the received data
        # For demonstration purposes, just echoing back the received data
        response = {"message": "Received data successfully", "data": data}

        # Sending back a JSON response
        return jsonify(response)
    else:
        # Method Not Allowed
        return jsonify({"error": "Method not allowed"}), 405


@webserver.route('/api/get_results/<job_id>', methods=['GET'])
def get_response(job_id):
    print(f"JobID is {job_id}")
    # TODO


    #print (job_id.split("_")[2])

    convertedJobId = int(job_id.split("_")[2]) - 1;

    # Check if job_id is valid
    # if the job id is greater than the counter
    # of ids assigned until this moment
    # it is not valid
    if convertedJobId > assigned_job_id:
        return jsonify({
            "status": "error",
            "reason": "Invalid job_id"
        })


    # Check if job_id is done and return the result
    #    res = res_for(job_id)
    #    return jsonify({
    #        'status': 'done',
    #        'data': res
    #    })

    jobsWrapper = JobsWrapper()


    if len(jobsWrapper.finishedJobs) > 0:

        if convertedJobId in jobsWrapper.finishedJobs:
            #print("INTRA IN FINISHED")
            res = jobsWrapper.finishedJobs[convertedJobId]
            return jsonify({
                'status': 'done',
                'data': res
            })


    # return jsonify({
    #     'status': 'done',
    #    'data': 'DATA'
    # })

    #print("\nINTRA IN RUNNING")
    # the job is valid, but not done
    return jsonify({'status': 'running'})



@webserver.route('/api/states_mean', methods=['POST'])
def states_mean_request():
    # Get request data
    data = request.json
    print(f"Got request {data}")

    global assigned_job_id

    # TODO
    # Register job. Don't wait for task to finish
    job = Job(assigned_job_id, 'statesMeanRequest', data["question"])
    threadPool.tasks.put(job)

    # Increment job_id counter
    assigned_job_id += 1
    # Return associated job_id
    return jsonify({'job_id': f"job_id_{assigned_job_id}"})

    # return jsonify({"status": "NotImplemented"})


@webserver.route('/api/state_mean', methods=['POST'])
def state_mean_request():
    data = request.json
    #print(f"Got request {data}")

    global assigned_job_id

    # TODO
    # Register job. Don't wait for task to finish
    job = Job(assigned_job_id, 'stateMeanRequest', data["question"], data["state"])
    threadPool.tasks.put(job)

    # Increment job_id counter
    assigned_job_id += 1
    # Return associated job_id
    return jsonify({'job_id': f"job_id_{assigned_job_id}"})


@webserver.route('/api/best5', methods=['POST'])
def best5_request():
    # Get request data
    data = request.json
    print(f"Got request {data}")

    global assigned_job_id

    # TODO
    # Register job. Don't wait for task to finish
    job = Job(assigned_job_id, 'best5Request', data["question"])
    threadPool.tasks.put(job)

    # Increment job_id counter
    assigned_job_id += 1
    # Return associated job_id
    return jsonify({'job_id': f"job_id_{assigned_job_id}"})


@webserver.route('/api/worst5', methods=['POST'])
def worst5_request():
    # Get request data
    data = request.json
    print(f"Got request {data}")

    global assigned_job_id

    # TODO
    # Register job. Don't wait for task to finish
    job = Job(assigned_job_id, 'worst5Request', data["question"])
    threadPool.tasks.put(job)

    # Increment job_id counter
    assigned_job_id += 1
    # Return associated job_id
    return jsonify({'job_id': f"job_id_{assigned_job_id}"})


@webserver.route('/api/global_mean', methods=['POST'])
def global_mean_request():
    data = request.json
    # print(f"Got request {data}")

    global assigned_job_id

    # TODO
    # Register job. Don't wait for task to finish
    job = Job(assigned_job_id, 'globalMeanRequest', data["question"])
    threadPool.tasks.put(job)

    # Increment job_id counter
    assigned_job_id += 1
    # Return associated job_id
    return jsonify({'job_id': f"job_id_{assigned_job_id}"})


@webserver.route('/api/diff_from_mean', methods=['POST'])
def diff_from_mean_request():
    data = request.json
    # print(f"Got request {data}")

    global assigned_job_id

    # TODO
    # Register job. Don't wait for task to finish
    job = Job(assigned_job_id, 'diffFromMeanRequest', data["question"])
    threadPool.tasks.put(job)

    # Increment job_id counter
    assigned_job_id += 1
    # Return associated job_id
    return jsonify({'job_id': f"job_id_{assigned_job_id}"})


@webserver.route('/api/state_diff_from_mean', methods=['POST'])
def state_diff_from_mean_request():
    # Get request data
    data = request.json
    #print(f"Got request {data}")

    global assigned_job_id

    # TODO
    # Register job. Don't wait for task to finish
    job = Job(assigned_job_id, 'statesDiffFromMeanRequest', data["question"], data["state"])
    threadPool.tasks.put(job)

    # Increment job_id counter
    assigned_job_id += 1
    # Return associated job_id
    return jsonify({'job_id': f"job_id_{assigned_job_id}"})


@webserver.route('/api/mean_by_category', methods=['POST'])
def mean_by_category_request():
    # TODO
    # Get request data
    # Register job. Don't wait for task to finish
    # Increment job_id counter
    # Return associated job_id

    return jsonify({"status": "NotImplemented"})


@webserver.route('/api/state_mean_by_category', methods=['POST'])
def state_mean_by_category_request():
    # TODO
    # Get request data
    # Register job. Don't wait for task to finish
    # Increment job_id counter
    # Return associated job_id

    return jsonify({"status": "NotImplemented"})


# You can check localhost in your browser to see what this displays
@webserver.route('/')
@webserver.route('/index')
def index():
    routes = get_defined_routes()
    msg = f"Hello, World!\n Interact with the webserver using one of the defined routes:\n"

    # Display each route as a separate HTML <p> tag
    paragraphs = ""
    for route in routes:
        paragraphs += f"<p>{route}</p>"

    msg += paragraphs
    return msg


def get_defined_routes():
    routes = []
    for rule in webserver.url_map.iter_rules():
        methods = ', '.join(rule.methods)
        routes.append(f"Endpoint: \"{rule}\" Methods: \"{methods}\"")
    return routes
