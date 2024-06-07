from boto3 import client
from shimoku_tangram.logging import init_logger
from gzip import compress as compress_f, uncompress
import json

logger = init_logger(__name__)

def get_object(
    bucket: str, 
    key: str,
    compressed: bool = True
) -> bytes:
    s3 = client('s3')
    response = s3.get_object(Bucket=bucket, Key=key)
    if compressed:    
        return uncompress(response['Body'].read())
    else:
        return response['Body'].read()

def list_objects(
    bucket: str,
    prefix: str = '',
    compressed: bool = True
) -> list:
    s3 = client('s3')
    response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
    if 'Contents' not in response:
        return []
    return [
        obj['Body'].read() if compressed else uncompress(obj['Body'].read())
        for obj in response['Contents']
    ]

def put_object(
    bucket: str, 
    key: str, 
    body: bytes,
    compress: bool = False
) -> bool:
    if compress:
        body = compress_f(body)
    s3 = client('s3')
    response_code = s3.put_object(
        Bucket=bucket, Key=key, Body=body
    )['ResponseMetadata']['HTTPStatusCode']
    return response_code < 300 and response_code >= 200

def delete_object(
    bucket: str, 
    key: str
) -> bool:
    s3 = client('s3')
    response_code = s3.delete_object(
        Bucket=bucket, Key=key
    )['ResponseMetadata']['HTTPStatusCode']
    return response_code < 300 and response_code >= 200

def clear_path(
    bucket: str, 
    prefix: str = ''
) -> bool:
    result = True
    for obj in list_objects(bucket, prefix):
        if not delete_object(bucket, obj['Key']):
            result = False
            (f'Failed to delete {obj["Key"]}') 
    return result

def get_text_object(
    bucket: str, 
    key: str,
    encoding: str = 'utf-8',
    compressed: bool = True
) -> str:
    return get_object(bucket, key, compressed).decode(encoding)

def list_text_objects(
    bucket: str,
    prefix: str = '',
    encoding: str = 'utf-8',
    compressed: bool = True
) -> list:
    return [
        obj.decode(encoding) for obj in 
        list_objects(bucket, prefix, compressed)
    ]

def put_text_object(
    bucket: str, 
    key: str, 
    body: str,
    encoding: str = 'utf-8',
    compress: bool = True
) -> bool:
    return put_object(bucket, key, body.encode(encoding), compress)

def get_json_object(
    bucket: str, 
    key: str,
    compressed: bool = True
) -> dict:
    return json.loads(get_text_object(bucket, key, compressed))

def list_json_objects(
    bucket: str,
    prefix: str = '',
    compressed: bool = True
) -> list:
    return [
        json.loads(obj) for obj in 
        list_text_objects(bucket, prefix, compressed)
    ]

def put_json_object(
    bucket: str, 
    key: str, 
    body: dict,
    compress: bool = True
) -> bool:
    return put_text_object(bucket, key, json.dumps(body), compress) 
