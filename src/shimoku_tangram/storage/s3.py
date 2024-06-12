from boto3 import client
from shimoku_tangram.reporting.logging import init_logger
from gzip import compress as compress_f, decompress
import json

logger = init_logger(__name__)


def bucket_exists(bucket: str) -> bool:
    s3 = client("s3")
    try:
        s3.head_bucket(Bucket=bucket)
        return True
    except s3.exceptions.ClientError:
        return False


def list_objects_metadata(bucket: str, prefix: str = "") -> list:
    s3 = client("s3")
    response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
    if "Contents" not in response:
        return []
    return response["Contents"]


def list_objects_key(bucket: str, prefix: str = "") -> list:
    return [obj["Key"] for obj in list_objects_metadata(bucket, prefix)]


def clear_path(bucket: str, prefix: str = "") -> bool:
    result = True
    for obj_key in list_objects_key(bucket, prefix):
        result = result and delete_object(bucket, obj_key)
    return result


def get_object(bucket: str, key: str, compressed: bool = True) -> bytes:
    s3 = client("s3")
    response = s3.get_object(Bucket=bucket, Key=key)
    if compressed:
        return decompress(response["Body"].read())
    else:
        return response["Body"].read()


def put_object(bucket: str, key: str, body: bytes, compress: bool = True) -> bool:
    if compress:
        body = compress_f(body)
    s3 = client("s3")
    response_code = s3.put_object(Bucket=bucket, Key=key, Body=body)[
        "ResponseMetadata"
    ]["HTTPStatusCode"]
    return response_code < 300 and response_code >= 200


def delete_object(bucket: str, key: str) -> bool:
    s3 = client("s3")
    response_code = s3.delete_object(Bucket=bucket, Key=key)["ResponseMetadata"][
        "HTTPStatusCode"
    ]
    return response_code < 300 and response_code >= 200


def get_text_object(
    bucket: str, key: str, encoding: str = "utf-8", compressed: bool = True
) -> str:
    return get_object(bucket, key, compressed).decode(encoding)


def put_text_object(
    bucket: str, key: str, body: str, encoding: str = "utf-8", compress: bool = True
) -> bool:
    return put_object(bucket, key, body.encode(encoding), compress)


def get_json_object(bucket: str, key: str, compressed: bool = True) -> dict:
    return json.loads(get_text_object(bucket, key, compressed=compressed))


def put_json_object(bucket: str, key: str, body: dict, compress: bool = True) -> bool:
    return put_text_object(bucket, key, json.dumps(body), compress=compress)
