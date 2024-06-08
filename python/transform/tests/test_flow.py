# import os
# import unittest
# import pandas as pd
# import pyarrow.parquet as pq
# import boto3
# from botocore.exceptions import NoCredentialsError
# from transform.processor import Processor
# from transform.s3_file_manager import S3FilesManager
# from transform.business_mapper import BusinessMapper
# from transform.state_manager import SimpleState
# from testcontainers.localstack import LocalStackContainer
#
# RESOURCES_DIR = os.path.join(os.path.dirname(__file__), "resources")
# INPUT_DIR = os.path.join(RESOURCES_DIR, "input")
# OUTPUT_DIR = os.path.join(RESOURCES_DIR, "output")
# RESULTS_DIR = os.path.join(RESOURCES_DIR, "results")
# STATE_FILE_NAME = "state.data"
# BUCKET_NAME = "test-bucket"
#
#
# def setup_state_file(path, content):
#     """Creates a state file with the given content."""
#     try:
#         with open(path, "w") as f:
#             f.write(content)
#     except Exception as e:
#         print(f"Error setting up state file: {e}")
#
#
# def clean_state_file(path):
#     """Removes the state file."""
#     try:
#         os.remove(path)
#     except FileNotFoundError:
#         print(f"State file {path} not found for cleanup.")
#
#
# def clean_up_outputs():
#     """Cleans up the output directory."""
#     try:
#         files = os.listdir(OUTPUT_DIR)
#         for file in files:
#             os.remove(os.path.join(OUTPUT_DIR, file))
#     except FileNotFoundError:
#         print(f"Output directory {OUTPUT_DIR} not found for cleanup.")
#
#
# class TestProcessorWithS3(unittest.TestCase):
#     def setUp(self):
#         """Sets up the necessary directories and LocalStack before each test."""
#         os.makedirs(OUTPUT_DIR, exist_ok=True)
#         self.localstack = LocalStackContainer(image="localstack/localstack:latest")
#         self.localstack.start()
#
#         self.s3_client = boto3.client(
#             "s3",
#             region_name=self.localstack.region_name,
#             endpoint_url=self.localstack.get_endpoint_url("s3"),
#             aws_access_key_id=self.localstack.access_key,
#             aws_secret_access_key=self.localstack.secret_key,
#         )
#
#         self.s3_client.create_bucket(Bucket=BUCKET_NAME)
#
#     def tearDown(self):
#         """Removes the output directory and stops LocalStack after each test."""
#         os.rmdir(OUTPUT_DIR)
#         self.localstack.stop()
#
#     test_cases = [
#         {
#             "test_name": "empty state file, has 4 users with one duplicate -> expect 3 users in Parquet",
#             "state_file_content": "",
#             "expected_result_file": "3_users.parquet"
#         },
#         {
#             "test_name":
#                 "with state file, has 2 users with no duplicate -> expect 2 users in Parquet",
#             "state_file_content": "1717298290.json",
#             "expected_result_file": "2_users.parquet"
#         },
#     ]
#
#     def run_test_case(self, state_file_content, expected_result_file):
#         """Runs a single test case."""
#         state_file_path = os.path.join(INPUT_DIR, STATE_FILE_NAME)
#         setup_state_file(state_file_path, state_file_content)
#
#         manager = S3FilesManager(bucket_name=BUCKET_NAME, s3_client=self.s3_client)
#         mapper = BusinessMapper("user_data.json", "company_data.json")
#         state = SimpleState(state_file_path, manager)
#
#         processor = Processor(manager, mapper, manager, state)
#         processor.process(INPUT_DIR, OUTPUT_DIR)
#
#         generated_files = [f for f in os.listdir(OUTPUT_DIR) if f.endswith('.parquet')]
#         self.assertEqual(len(generated_files), 1, "There should be one generated Parquet file.")
#         generated_parquet_path = os.path.join(OUTPUT_DIR, generated_files[0])
#         table_generated = pq.read_table(generated_parquet_path)
#
#         expected_parquet_path = os.path.join(RESULTS_DIR, expected_result_file)
#         table_expected = pq.read_table(expected_parquet_path)
#
#         pd.testing.assert_frame_equal(table_expected.to_pandas(), table_generated.to_pandas(), check_like=True)
#
#         # Cleanup after each subtest
#         clean_up_outputs()
#         clean_state_file(state_file_path)
#
#     def test_processor(self):
#         """Runs all test cases."""
#         for case in self.test_cases:
#             with self.subTest(case=case["test_name"]):
#                 self.run_test_case(case["state_file_content"], case["expected_result_file"])
#
#
# if __name__ == "__main__":
#     unittest.main()
