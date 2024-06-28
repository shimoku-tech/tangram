from boto3 import client


def put_parameter(name: str, value: str):
    ssm_client = client("ssm")
    response = ssm_client.put_parameter(
        Name=name, Value=value, Type="SecureString", Overwrite=True
    )
    return response


def get_parameter(name: str):
    ssm_client = client("ssm")
    response = ssm_client.get_parameter(Name=name, WithDecryption=True)
    secret_parameter = response["Parameter"]["Value"]
    return secret_parameter


def delete_parameter(name: str):
    ssm_client = client("ssm")
    response = ssm_client.delete_parameter(Name=name)
    return response
