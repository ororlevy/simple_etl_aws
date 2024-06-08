import os
import unittest

import boto3
import pandas as pd
import pyarrow.parquet as pq
from transform.processor import Processor
from transform.tests.local_file_system import LocalFilesManager
from transform.tests.simple_mapper import SimpleMapper
from transform.state_manager import SimpleState
from transform.s3_file_manager import S3FilesManager
from testcontainers.localstack import LocalStackContainer
import pyarrow as pa

RESOURCES_DIR = os.path.join(os.path.dirname(__file__), "resources")
INPUT_DIR = os.path.join(RESOURCES_DIR, "input")
OUTPUT_DIR = os.path.join(RESOURCES_DIR, "output")
RESULTS_DIR = os.path.join(RESOURCES_DIR, "results")
STATE_FILE_NAME = "state.data"
INPUT_BUCKET_NAME = "raw-data"
OUTPUT_BUCKET_NAME = "transformed-data"





class TestProcessor(unittest.TestCase):
    def setUp(self):
        """Sets up the necessary directories before each test."""
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        self.localstack = LocalStackContainer(image="localstack/localstack:latest")
        self.localstack.start()

        self.s3_client = boto3.client(
            "s3",
            region_name=self.localstack.region_name,
            endpoint_url=self.localstack.get_url(),
            aws_access_key_id="test",
            aws_secret_access_key="test",
        )

        self.s3_client.create_bucket(Bucket=INPUT_BUCKET_NAME,
                                     CreateBucketConfiguration={'LocationConstraint': self.localstack.region_name})
        self.s3_client.create_bucket(Bucket=OUTPUT_BUCKET_NAME,
                                     CreateBucketConfiguration={'LocationConstraint': self.localstack.region_name})
        self.upload_files_to_s3(INPUT_BUCKET_NAME, INPUT_DIR)

    def upload_files_to_s3(self, bucket_name, directory):
        """Uploads all files from a local directory to an S3 bucket."""
        for filename in os.listdir(directory):
            file_path = os.path.join(directory, filename)
            with open(file_path, "rb") as data:
                if os.path.isfile(file_path):
                    self.s3_client.put_object(Bucket=bucket_name, Key=filename, Body=data)

    def tearDown(self):
        """Removes the output directory and stops LocalStack after all the tests."""
        files = os.listdir(OUTPUT_DIR)
        for file in files:
            os.remove(os.path.join(OUTPUT_DIR, file))
        os.rmdir(OUTPUT_DIR)
        self.localstack.stop()

    def run_test_case(self, state_file_content, table_expected, downloader, uploader):
        """Runs a single test case."""
        downloader.upload_file(STATE_FILE_NAME, state_file_content.encode("utf-8"))

        mapper = SimpleMapper()
        state = SimpleState(STATE_FILE_NAME, downloader)

        processor = Processor(downloader, mapper, uploader, state)
        processor.process()

        generated_files = [f for f in uploader.list_files() if f.endswith('.parquet')]
        self.assertEqual(len(generated_files), 1, "There should be one generated Parquet file.")
        table_generated = pq.read_table(pa.BufferReader(uploader.download_file(generated_files[0])))

        pd.testing.assert_frame_equal(table_expected.to_pandas(), table_generated.to_pandas(), check_like=True)

        # Cleanup after each subtest
        self.clean_up_outputs()
        self.clean_out_state()

    def clean_up_outputs(self):
        """Cleans up the output directory."""
        try:
            files = os.listdir(OUTPUT_DIR)
            for file in files:
                os.remove(os.path.join(OUTPUT_DIR, file))
        except FileNotFoundError:
            print(f"Output directory {OUTPUT_DIR} not found for cleanup.")

        response = self.s3_client.list_objects_v2(Bucket=OUTPUT_BUCKET_NAME, Prefix="")
        if 'Contents' in response:
            for obj in response['Contents']:
                self.s3_client.delete_object(Bucket=OUTPUT_BUCKET_NAME, Key=obj['Key'])

    def clean_out_state(self):
        try:
            os.remove(os.path.join(INPUT_DIR, STATE_FILE_NAME))
        except FileNotFoundError:
            print(f"State file  not found for cleanup.")

        self.s3_client.delete_object(Bucket=INPUT_BUCKET_NAME, Key=STATE_FILE_NAME)

    def test_processor(self):
        """Runs all test cases."""
        file_manager_download = LocalFilesManager(INPUT_DIR)
        file_manager_upload = LocalFilesManager(OUTPUT_DIR)
        s3_manager_download = S3FilesManager(bucket_name=INPUT_BUCKET_NAME, s3_client=self.s3_client)
        s3_manager_upload = S3FilesManager(bucket_name=OUTPUT_BUCKET_NAME, s3_client=self.s3_client)

        test_cases = [
            {
                "test_name": "file-sys:empty state file, has 4 users with one duplicate -> expect 3 users in Parquet",
                "state_file_content": "",
                "file_manager_upload": file_manager_upload,
                "file_manager_download": file_manager_download,
                "expected_result_file": "3_users.Parquet"
            },
            {
                "test_name":
                    "file sys: with state file, has 2 users with no duplicate -> expect 2 users in Parquet",
                "state_file_content": "1717298290.json",
                "file_manager_upload": file_manager_upload,
                "file_manager_download": file_manager_download,
                "expected_result_file": "2_users.Parquet"
            },
            {
                "test_name": "s3:empty state file, has 4 users with one duplicate -> expect 3 users in Parquet",
                "state_file_content": "",
                "file_manager_upload": s3_manager_upload,
                "file_manager_download": s3_manager_download,
                "expected_result_file": "3_users.Parquet"
            },
            {
                "test_name":
                    "s3: with state file, has 2 users with no duplicate -> expect 2 users in Parquet",
                "state_file_content": "1717298290.json",
                "file_manager_upload": s3_manager_upload,
                "file_manager_download": s3_manager_download,
                "expected_result_file": "2_users.Parquet"
            },
        ]

        for case in test_cases:
            with self.subTest(case=case["test_name"]):
                with open(os.path.join(RESULTS_DIR, case["expected_result_file"]), 'rb') as file:
                    file_content = file.read()
                self.run_test_case(case["state_file_content"], pq.read_table(pa.BufferReader(file_content)),
                                   case["file_manager_download"], case["file_manager_upload"])


if __name__ == "__main__":
    unittest.main()
