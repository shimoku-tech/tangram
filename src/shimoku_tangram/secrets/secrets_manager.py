import boto3
import json
from shimoku_tangram.reporting.logging import init_logger
logger = init_logger(__name__)

def put_secret(
    secret_name: str,
    secret_value: str,
) -> bool:
    client = boto3.client('secretsmanager')
    if not isinstance(secret_value, str):
        secret_value = json.dumps(secret_value)
    result = client.create_secret(Name=secret_name, SecretString=secret_value)
    return result['ResponseMetadata']['HTTPStatusCode'] == 200

def get_secret(
    secret_name: str,
    is_dict: bool = False
) -> str | dict:
    client = boto3.client('secretsmanager')
    get_secret_value_response = client.get_secret_value(SecretId=secret_name)
    secret = get_secret_value_response['SecretString']
    if is_dict:
        secret = json.loads(secret) 
    return secret

def delete_secret(
    secret_name: str
) -> bool:
    client = boto3.client('secretsmanager')
    result = client.delete_secret(SecretId=secret_name, ForceDeleteWithoutRecovery=True)
    return result['ResponseMetadata']['HTTPStatusCode'] == 200