import json
from botocore.exceptions import ClientError
from boto3 import client


def send_task_success(task_token: str, output: dict):
    try:
        sfn_client = client("stepfunctions")
        sfn_client.send_task_success(taskToken=task_token, output=json.dumps(output))
    except ClientError as e:
        print("Error sending task success", e)


def send_task_failure(task_token: str, error_message: str):
    try:
        sfn_client = client("stepfunctions")
        sfn_client.send_task_failure(
            taskToken=task_token, error="TaskFailed", cause=error_message
        )
    except ClientError as e:
        print("Error sending task failure", e)
    