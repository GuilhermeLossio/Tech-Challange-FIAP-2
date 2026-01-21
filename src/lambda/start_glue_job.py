import boto3
import re
import os

glue = boto3.client("glue")
JOB_NAME = os.environ["GLUE_JOB_NAME"]

DT_RE = re.compile(r"raw/dt=(\d{4}-\d{2}-\d{2})/")

def lambda_handler(event, context):
    record = event["Records"][0]
    key = record["s3"]["object"]["key"]

    dt = DT_RE.search(key).group(1)

    glue.start_job_run(
        JobName=JOB_NAME,
        Arguments={"--dt": dt}
    )
