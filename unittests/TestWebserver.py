import json
import os
import unittest
from datetime import datetime
from time import sleep


import requests
from deepdiff import DeepDiff




class TestWebserver(unittest.TestCase):

    def setUp(self):
        pass

    def check_res_timeout(self, res_callable, ref_result, timeout_sec, poll_interval=0.2):
        initial_timestamp = datetime.now()
        while True:
            response = res_callable()

            # Asserting that the response status code is 200 (OK)
            self.assertEqual(response.status_code, 200)

            # Asserting the response data
            response_data = response.json()
            if response_data['status'] == 'done':
                d = DeepDiff(response_data['data'], ref_result, math_epsilon=0.01)
                self.assertTrue(d == {}, str(d))
                break
            elif response_data['status'] == 'running':
                current_timestamp = datetime.now()
                time_delta = current_timestamp - initial_timestamp
                if time_delta.seconds > timeout_sec:
                    self.fail("Operation timedout")
                else:
                    sleep(poll_interval)

    def helper_test_endpoint(self, endpoint):
        output_dir = f"{endpoint}/output/"
        input_dir = f"{endpoint}/input/"
        input_files = os.listdir(input_dir)

        for input_file in input_files:
            # Get the index from in-idx.json
            # The idx is between a dash (-) and a dot (.)
            idx = input_file.split('-')[1]
            idx = int(idx.split('.')[0])

            with open(f"{input_dir}/{input_file}", "r") as fin:
                # Data to be sent in the POST request
                req_data = json.load(fin)

            with open(f"{output_dir}/out-{idx}.json", "r") as fout:
                ref_result = json.load(fout)

            with self.subTest():
                # Sending a POST request to the Flask endpoint
                res = requests.post(f"http://127.0.0.1:5000/api/{endpoint}", json=req_data)

                job_id = res.json()
                # print(f'job-res is {job_id}')
                job_id = job_id["job_id"]

                self.check_res_timeout(
                    res_callable=lambda: requests.get(f"http://127.0.0.1:5000/api/get_results/{job_id}"),
                    ref_result=ref_result,
                    timeout_sec=1)

    def test_state_mean(self):
        self.helper_test_endpoint("state_mean")

    def test_states_mean(self):
        self.helper_test_endpoint("states_mean")

    def test_best5(self):
        self.helper_test_endpoint("best5")

    def test_worst5(self):
        self.helper_test_endpoint("worst5")

    def test_global_mean(self):
        self.helper_test_endpoint("global_mean")

    def test_diff_from_mean(self):
        self.helper_test_endpoint("diff_from_mean")

    def test_state_diff_from_mean(self):
        self.helper_test_endpoint("state_diff_from_mean")

    def test_mean_by_category(self):
        self.helper_test_endpoint("mean_by_category")

    def test_state_mean_by_category(self):
        self.helper_test_endpoint("state_mean_by_category")


if __name__ == "__main__":
    unittest.main()