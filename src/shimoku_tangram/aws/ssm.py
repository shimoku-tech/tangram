import boto3
from os import getenv


def get_ssm_client():
    return boto3.client('ssm')      


def put_secret(
    name: str, 
    value: str
):
    ssm_client = get_ssm_client()
    response = ssm_client.put_parameter(
        Name=name, Value=value, 
        Type='SecureString', Overwrite=True
    )
    return response

def get_secret(
    name: str
):
    ssm_client = get_ssm_client()
    response = ssm_client.get_parameter(
        Name=name, WithDecryption=True
    )
    secret_parameter = response['Parameter']['Value']
    return secret_parameter

def delete_secret(
    name: str
):
    ssm_client = get_ssm_client()
    response = ssm_client.delete_parameter(
        Name=name
    )
    return response
