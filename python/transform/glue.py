import sys

import boto3
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext

from transform.s3_file_manager import S3FilesManager
from transform.state_manager import SimpleState
from transform.processor import Processor
from transform.business_mapper import BusinessMapper

args = getResolvedOptions(sys.argv,
                          ['JOB_NAME', 'raw_bucket', 'transformed_bucket', 'state_file_name', 'user_data_fname',
                           'company_data_fname'])
raw_bucket = args['raw_bucket']
transformed_bucket = args['transformed_bucket']
state_file_name = args['state-_file_name']
user_data_fname = args['user_data_fname']
company_data_fname = args['company_data_fname']
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

s3_client = boto3.client('s3')

getter = S3FilesManager(raw_bucket, s3_client)
uploader = S3FilesManager(transformed_bucket, s3_client)
state_manager = SimpleState(state_file_name, getter)
mapper = BusinessMapper(user_data_fname, company_data_fname)

processor = Processor(getter, mapper, uploader, state_manager)

processor.process()

job.commit()
