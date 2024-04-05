from app import webserver
from flask import request, jsonify

from .database import Database

import os
import json


# helper class to save the details from a received request
class Job:

    def __init__(self, job_id, request_type, request_question, state=None):
        self.requestType = request_type
        self.requestQuestion = request_question
        # used for ordering in best5 and worst5
        self.state = state
        self.jobId = job_id


@webserver.route('/api/get_results/<job_id>', methods=['GET'])
def get_response(job_id):
    database = Database()

    database.output_log(f"Received get_results request with {job_id}")

    status = database.get_job_status(job_id)

    # Check if job_id is valid
    if status == 'invalid id':
        database.output_log(f"Exited get_results request with {job_id}")
        return jsonify({
            "status": "error",
            "reason": "Invalid job_id"
        })

    # the job is valid and done
    if status == 'done':
        current_dir = os.getcwd()
        output_path = os.path.join(current_dir, "results", f"{job_id}.json")

        with open(output_path, 'r') as file:
            res = json.load(file)
            database.output_log(f"Exited get_results request with {job_id}")
            return jsonify({
                'status': 'done',
                'data': res
            })

    # the job is valid, but still running
    database.output_log(f"Exited get_results request with {job_id}")
    return jsonify({'status': 'running'})


@webserver.route('/api/jobs', methods=['GET'])
def get_jobs():
    database = Database()

    database.output_log("Received get_jobs request")

    res = database.job_status

    database.output_log("Exited get_jobs request")

    return jsonify({
        'status': 'done',
        'data': res
    })


@webserver.route('/api/num_jobs', methods=['GET'])
def get_num_jobs():
    database = Database()

    database.output_log("Received get_num_jobs request")

    jobs_left = database.get_jobs_left()

    database.output_log("Exited get_num_jobs request")

    return jsonify({
        'status': 'done',
        'data': jobs_left
    })


@webserver.route('/api/graceful_shutdown', methods=['GET'])
def graceful_shutdown_request():
    database = Database()

    database.output_log("Received graceful_shutdown request")

    database.shutdown()

    database.output_log("Exited graceful_shutdown request")

    return jsonify({
        'status': 'shutting down'
    })


@webserver.route('/api/states_mean', methods=['POST'])
def states_mean_request():
    database = Database()
    data = request.json

    database.output_log(f"Received states_mean request with question: {data['question']}")

    if not database.shutdown:
        # get an id assigned to the current request
        assigned_job_id = database.assign_job_id()

        # add the job to the queue
        job = Job(assigned_job_id, 'statesMeanRequest', data["question"])
        database.tasks.put(job)

        # Return associated job_id
        database.output_log("Exited states_mean request")
        return jsonify({'job_id': f"job_id_{assigned_job_id}"})

    database.output_log("Exited states_mean request")
    return jsonify({
        'job_id': -1,
        'reason': 'shutting down'
    })


@webserver.route('/api/state_mean', methods=['POST'])
def state_mean_request():
    database = Database()
    data = request.json

    database.output_log(f"Received state_mean request with question: {data['question']} and state: {data['state']}")

    if not database.shutdown:
        # get an id assigned to the current request
        assigned_job_id = database.assign_job_id()

        # add the job to the queue
        job = Job(assigned_job_id, 'stateMeanRequest', data["question"], data["state"])
        database.tasks.put(job)

        # Return associated job_id
        database.output_log("Exited state_mean request")
        return jsonify({'job_id': f"job_id_{assigned_job_id}"})

    database.output_log("Exited state_mean request")
    return jsonify({
        'job_id': -1,
        'reason': 'shutting down'
    })


@webserver.route('/api/best5', methods=['POST'])
def best5_request():
    database = Database()
    data = request.json

    database.output_log(f"Received best5 request with question: {data['question']}")

    if not database.shutdown:
        # get an id assigned to the current request
        assigned_job_id = database.assign_job_id()

        # add the job to the queue
        job = Job(assigned_job_id, 'best5Request', data["question"])
        database.tasks.put(job)

        # Return associated job_id
        database.output_log("Exited best5 request")
        return jsonify({'job_id': f"job_id_{assigned_job_id}"})

    database.output_log("Exited best5 request")
    return jsonify({
        'job_id': -1,
        'reason': 'shutting down'
    })


@webserver.route('/api/worst5', methods=['POST'])
def worst5_request():
    database = Database()
    data = request.json

    database.output_log(f"Received worst5 request with question: {data['question']}")

    if not database.shutdown:
        # get an id assigned to the current request
        assigned_job_id = database.assign_job_id()

        # add the job to the queue
        job = Job(assigned_job_id, 'worst5Request', data["question"])
        database.tasks.put(job)

        # Return associated job_id
        database.output_log("Exited worst5 request")
        return jsonify({'job_id': f"job_id_{assigned_job_id}"})

    database.output_log("Exited worst5 request")
    return jsonify({
        'job_id': -1,
        'reason': 'shutting down'
    })


@webserver.route('/api/global_mean', methods=['POST'])
def global_mean_request():
    database = Database()
    data = request.json

    database.output_log(f"Received global_mean request with question: {data['question']}")

    if not database.shutdown:
        # get an id assigned to the current request
        assigned_job_id = database.assign_job_id()

        # add the job to the queue
        job = Job(assigned_job_id, 'globalMeanRequest', data["question"])
        database.tasks.put(job)

        # Return associated job_id
        database.output_log("Exited global_mean request")
        return jsonify({'job_id': f"job_id_{assigned_job_id}"})

    database.output_log("Exited global_mean request")
    return jsonify({
        'job_id': -1,
        'reason': 'shutting down'
    })


@webserver.route('/api/diff_from_mean', methods=['POST'])
def diff_from_mean_request():
    database = Database()
    data = request.json

    database.output_log(f"Received diff_from_mean request with question: {data['question']}")

    if not database.shutdown:
        # get an id assigned to the current request
        assigned_job_id = database.assign_job_id()

        # add the job to the queue
        job = Job(assigned_job_id, 'diffFromMeanRequest', data["question"])
        database.tasks.put(job)

        # Return associated job_id
        database.output_log("Exited diff_from_mean request")
        return jsonify({'job_id': f"job_id_{assigned_job_id}"})

    database.output_log("Exited diff_from_mean request")
    return jsonify({
        'job_id': -1,
        'reason': 'shutting down'
    })


@webserver.route('/api/state_diff_from_mean', methods=['POST'])
def state_diff_from_mean_request():
    database = Database()
    data = request.json

    database.output_log(
        f"Received state_diff_from_mean request with question: {data['question']} and state: {data['state']}")

    if not database.shutdown:
        # get an id assigned to the current request
        assigned_job_id = database.assign_job_id()

        # add the job to the queue
        job = Job(assigned_job_id, 'statesDiffFromMeanRequest', data["question"], data["state"])
        database.tasks.put(job)

        # Return associated job_id
        database.output_log("Exited state_diff_from_mean request")
        return jsonify({'job_id': f"job_id_{assigned_job_id}"})

    database.output_log("Exited state_diff_from_mean request")
    return jsonify({
        'job_id': -1,
        'reason': 'shutting down'
    })


@webserver.route('/api/mean_by_category', methods=['POST'])
def mean_by_category_request():
    database = Database()
    data = request.json

    database.output_log(f"Received mean_by_category request with question: {data['question']}")

    if not database.shutdown:
        # get an id assigned to the current request
        assigned_job_id = database.assign_job_id()

        # add the job to the queue
        job = Job(assigned_job_id, 'meanByCategoryRequest', data["question"])
        database.tasks.put(job)

        # Return associated job_id
        database.output_log("Exited mean_by_category request")
        return jsonify({'job_id': f"job_id_{assigned_job_id}"})

    database.output_log("Exited mean_by_category request")
    return jsonify({
        'job_id': -1,
        'reason': 'shutting down'
    })


@webserver.route('/api/state_mean_by_category', methods=['POST'])
def state_mean_by_category_request():
    database = Database()
    data = request.json

    database.output_log(
        f"Received state_mean_by_category request with question: {data['question']} and state: {data['state']}")

    if not database.shutdown:
        # get an id assigned to the current request
        assigned_job_id = database.assign_job_id()

        # add the job to the queue
        job = Job(assigned_job_id, 'stateMeanByCategoryRequest', data["question"], data["state"])
        database.tasks.put(job)

        # Return associated job_id
        database.output_log("Exited state_mean_by_category request")
        return jsonify({'job_id': f"job_id_{assigned_job_id}"})

    database.output_log("Exited state_mean_by_category request")
    return jsonify({
        'job_id': -1,
        'reason': 'shutting down'
    })


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
