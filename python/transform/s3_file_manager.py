from typing import List
from transform.interfaces import FilesManager
import boto3


class S3FilesManager(FilesManager):
    def __init__(self, bucket_name: str, s3_client: boto3.client):
        self.s3 = s3_client
        self.bucket_name = bucket_name

    def list_files(self) -> List[str]:
        response = self.s3.list_objects_v2(Bucket=self.bucket_name, Prefix="")
        return [item['Key'] for item in response.get('Contents', [])]

    def download_file(self, file_name: str) -> bytes:
        response = self.s3.get_object(Bucket=self.bucket_name, Key=file_name)
        return response['Body'].read()

    def upload_file(self, file_name: str, data: bytes) -> None:
        self.s3.put_object(Bucket=self.bucket_name, Key=file_name, Body=data)
