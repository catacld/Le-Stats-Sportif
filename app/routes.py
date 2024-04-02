from app import webserver
from flask import request, jsonify

from .database import Database
from .task_runner import ThreadPool

import os
import json

assigned_job_id = 1
threadPool = ThreadPool()


class Job:

    def __init__(self, requestId, requestType, requestQuestion, state=None):
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

    # -1 since my ids starts from 1 and server ids from 2
    convertedJobId = int(job_id.split("_")[2]) - 1
    jobId = 'job_id_' + str(convertedJobId)
    database = Database()

    # Check if job_id is valid
    if jobId not in database.jobStatus:
        return jsonify({
            "status": "error",
            "reason": "Invalid job_id"
        })




    # the job is valid and done
    if database.jobStatus[jobId] == 'done':
        current_dir = os.getcwd()
        output_path = os.path.join(current_dir, "results", f"job_id_{convertedJobId}")
        with open(output_path, 'r') as file:
            res = json.load(file)

            return jsonify({
                'status': 'done',
                'data': res
            })

    # the job is valid, but still running
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


@webserver.route('/api/state_mean', methods=['POST'])
def state_mean_request():
    data = request.json

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
    # print(f"Got request {data}")

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
    data = request.json
    print(f"Got request {data}")

    global assigned_job_id

    # TODO
    # Register job. Don't wait for task to finish
    job = Job(assigned_job_id, 'meanByCategoryRequest', data["question"])
    threadPool.tasks.put(job)

    # Increment job_id counter
    assigned_job_id += 1
    # Return associated job_id
    return jsonify({'job_id': f"job_id_{assigned_job_id}"})


@webserver.route('/api/state_mean_by_category', methods=['POST'])
def state_mean_by_category_request():
    data = request.json
    # print(f"Got request {data}")

    global assigned_job_id

    # TODO
    # Register job. Don't wait for task to finish
    job = Job(assigned_job_id, 'stateMeanByCategoryRequest', data["question"], data["state"])
    threadPool.tasks.put(job)

    # Increment job_id counter
    assigned_job_id += 1
    # Return associated job_id
    return jsonify({'job_id': f"job_id_{assigned_job_id}"})


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
