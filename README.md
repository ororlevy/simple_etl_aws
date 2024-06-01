# Simple Serverless ETL with AWS

### TL;DR
That's a simple ETL pipeline using EventBridge, Lambdas and Glue over S3. 
Including CI with Github Actions and integration tests using LocalStack.

## ETL Pipeline
### Extract
#### Lambda Function
- Extracts JSON data from an API and dumps it into an S3 bucket.
- writes files based on size and time

#### Raw Bucket
The Lambda function will put the data into a bucket with the following structure:

- Expected file paths:
```${year}/${month}/${day}/${hour}/${epoch_timestamp}.json```
- Default retention is 1 day.
### Transform
Controlled by EventBridge - triggered when the lambda function finished

Glue Job Responsibilities:
- Processing: Filter and modify data according to business logic.
- Output: Create Parquet files and store them in a new bucket.
- State Management: Update the state file with the latest timestamp.
- Error Handling: Implement an at-least-once strategy.
- Post-Processing: Trigger another Lambda function to process the Parquet files.
### Load
#### Lambda Function:
- Triggered by the Glue job.
- Loads the Parquet files into an AWS Redshift data warehouse.
