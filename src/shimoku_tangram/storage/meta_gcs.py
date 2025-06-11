from datetime import datetime
from shimoku_tangram.storage import gcs


def get_last_timestamp(bucket: str, prefix: str) -> str:
    last_timestamp_key: str = f"{prefix}/metadata/lastTimestamp.txt.gz"
    last_timestamp: str = gcs.get_text_object(bucket=bucket, key=last_timestamp_key)
    return last_timestamp


def set_last_timestamp(bucket: str, prefix: str) -> str:
    last_timestamp: str = datetime.now().strftime("%Y-%m-%d:%H:%M:%S")
    last_timestamp_key: str = f"{prefix}/metadata/lastTimestamp.txt.gz"
    gcs.put_text_object(bucket=bucket, key=last_timestamp_key, body=last_timestamp)
    return last_timestamp
