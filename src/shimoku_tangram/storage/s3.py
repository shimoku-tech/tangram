from gzip import compress as compress_f, decompress
import io
import json
import os
import logging
from pathlib import Path
import pickle
from typing import Dict, List
import uuid
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor
import csv

from boto3 import client
import pandas as pd

from shimoku_tangram.reporting.logging import init_logger


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


def list_objects_key_between_dates(
    bucket: str,
    prefix: str, 
    start_date: datetime, 
    end_date: datetime
) -> List[str]:
    """
    List all objects keys between two dates.
    The date is extracted from the object key, which has the following format:
    <prefix>/<YYYY>/<MM>/<DD>/<filename>
    """
    end_date = end_date + timedelta(days=1)
    file_prefixes = []
    for x in range(0, (end_date-start_date).days):
        date = start_date + timedelta(days=x)
        next_date = date + timedelta(days=1)
        file_prefixes.append(f"{prefix}/{date.strftime('%Y/%m/%d')}")

        if (next_date.month == 1 and next_date.day == 1 and 
            f"{prefix}/{date.strftime('%Y')}/01" in file_prefixes):
            file_prefixes = [
                file_prefix 
                for file_prefix in file_prefixes 
                if not file_prefix.startswith(f"{prefix}/{date.strftime('%Y')}")
            ]
            file_prefixes.append(f"{prefix}/{date.strftime('%Y')}")
        elif (next_date.day == 1 and 
           f"{prefix}/{date.strftime('%Y/%m')}/01" in file_prefixes):
            file_prefixes = [
                file_prefix 
                for file_prefix in file_prefixes 
                if not file_prefix.startswith(f"{prefix}/{date.strftime('%Y/%m')}")
            ]
            file_prefixes.append(f"{prefix}/{date.strftime('%Y/%m')}")

    file_keys = []
    with ThreadPoolExecutor() as executor:
        tasks = [
            executor.submit(list_objects_key, bucket, file_prefix)
            for file_prefix in file_prefixes
        ]
        for task in tasks:
            file_keys.extend(task.result())

    return file_keys


def list_single_object_key(bucket: str, prefix: str) -> str:
    """
    Retrieve the single file key from an S3 bucket given a folder.
    Fails if there are no files or more than one file is found.
    """
    list_keys = list_objects_key(bucket, prefix)

    list_keys = [
        path for path in list_keys if Path(path).resolve() != Path(prefix).resolve()
    ]

    if len(list_keys) == 0:
        raise ValueError("File not found.")
    elif len(list_keys) > 1:
        raise ValueError("Multiple files found.")

    return list_keys[0]


def list_multiple_objects_keys(bucket: str, prefix: str) -> List[str]:
    """
    Retrieve multiple file keys from an S3 bucket given a folder.
    Uses pagination to handle large numbers of objects.
    Fails if no files are found.
    """
    s3 = client("s3")
    paginator = s3.get_paginator('list_objects_v2')

    list_keys = []

    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        if 'Contents' in page:
            for obj in page['Contents']:
                if not obj['Key'].endswith('/'):
                    list_keys.append(obj['Key'])

    if len(list_keys) == 0:
        raise ValueError(f"No files found in prefix: {prefix}")

    return list_keys


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


def get_pkl_object(bucket: str, key: str, compressed: bool = True) -> dict:
    return pickle.loads(get_object(bucket, key, compressed))


def put_pkl_object(bucket: str, key: str, body, compress: bool = True):
    return put_object(bucket, key, body=pickle.dumps(body), compress=compress)


def get_extension(key: str, compressed: bool = True) -> str:
    if compressed:
        path_without_compressed_extension = os.path.splitext(key)[0]
        extension = os.path.splitext(path_without_compressed_extension)[1][1:]
        return extension
    else:
        extension = os.path.splitext(key)[1][1:]
        return extension


def is_compressed(key: str) -> bool:
    compressed_extensions = [".gz"]
    return any(key.endswith(ext) for ext in compressed_extensions)


def get_single_json_object(bucket: str, prefix: str):
    """
    Retrieve a single json object from an S3 bucket given a folder
    """
    key = list_single_object_key(bucket, prefix)

    if get_extension(key, compressed=is_compressed(key)) != "json":
        raise ValueError("File is not a json file.")

    return get_json_object(bucket, key=key, compressed=is_compressed(key))


def get_single_pkl_object(bucket: str, prefix: str):
    """
    Retrieve a single pickle object from an S3 bucket given a folder
    """
    key = list_single_object_key(bucket, prefix)

    if get_extension(key, compressed=is_compressed(key)) != "pkl":
        raise ValueError("File is not a pickle file.")

    return get_pkl_object(bucket, key=key, compressed=is_compressed(key))


def get_multiple_csv_objects(bucket: str, prefix: str) -> pd.DataFrame:
    """
    Retrieve multiple csv objects from an S3 bucket given a folder and
    concatenate them.
    """
    list_keys = list_multiple_objects_keys(bucket, prefix)

    list_ext = [
        get_extension(key=key, compressed=is_compressed(key)) for key in list_keys
    ]
    if not all(ext == "csv" for ext in list_ext):
        raise ValueError("Not all files are csv files.")

    list_df = []
    for key in list_keys:
        list_df += [
            pd.read_csv(
                io.StringIO(
                    get_text_object(bucket, key=key, compressed=is_compressed(key))
                )
            )
        ]

    try:
        df = pd.concat(list_df).reset_index(drop=True)
    except Exception as e:
        raise ValueError("Error concatenating dataframes") from e

    return df


def put_single_json_object(bucket: str, prefix: str, body: Dict) -> str:
    """
    Clean folder and put a single json file into such folder.
    """
    clear_path(bucket, prefix)

    key = os.path.join(prefix, str(uuid.uuid4()) + ".json.gz")

    put_json_object(bucket, key=key, body=body, compress=True)

    return key


def put_single_pkl_object(bucket: str, prefix: str, body) -> str:
    """
    Clean folder and put a single pickle object into such folder.
    """
    clear_path(bucket, prefix)

    key = os.path.join(prefix, str(uuid.uuid4()) + ".pkl.gz")

    put_pkl_object(bucket, key=key, body=body, compress=True)

    return key


def put_multiple_csv_objects(
    bucket: str,
    prefix: str,
    body: pd.DataFrame,
    size_max_mb: float = 100,
) -> List[str]:
    """
    Clean folder, split dataframe into multiple csv files and put them into
    folder
    """
    clear_path(bucket, prefix)

    size_memory_mb = body.memory_usage(deep=True).sum() / (1024**2)
    size_row_slice = int(size_max_mb / size_memory_mb * len(body))

    def _generate_slices(dataframe: pd.DataFrame, row_slice_size: int):
        """Generates slices of the DataFrame based on the given row slice size."""
        for start in range(0, len(dataframe), row_slice_size):
            yield dataframe.iloc[start : start + row_slice_size]

    list_keys = list()
    for df in _generate_slices(body, max(size_row_slice, 1)):
        key = os.path.join(prefix, str(uuid.uuid4()) + ".csv.gz")
        list_keys.append(key)
        put_text_object(bucket, key=key, body=df.to_csv(index=False, quoting=csv.QUOTE_ALL), compress=True)

    return list_keys


def get_multiple_csv_objects_threaded(
    bucket: str, 
    prefixes: list[str],
    logger: logging.Logger | None = None
) -> pd.DataFrame:
    """
    Retrieve multiple csv objects from an S3 bucket given a list of prefixes and
    concatenate them.
    """
    def _get_object(key: str, list_df: list[pd.DataFrame]):
        if logger:
            logger.info(f"Getting object {key}")
        list_df.append(pd.read_csv(io.StringIO(
            get_text_object(bucket, key=key, compressed=is_compressed(key))
        )))
        if logger:
            logger.info(f"Object {key} done")

    def _get_keys(prefix: str, list_keys: list[str]):
        list_keys.extend(list_multiple_objects_keys(bucket, prefix))

    list_keys = []
    with ThreadPoolExecutor() as executor:
        for prefix in prefixes:
            executor.submit(_get_keys, prefix, list_keys)

    list_df = []
    with ThreadPoolExecutor() as executor:
        for key in list_keys:
            executor.submit(_get_object, key, list_df)

    if len(list_df) == 0:
        raise ValueError("No data found")

    try:
        df = pd.concat(list_df).reset_index(drop=True)
    except Exception as e:
        raise ValueError("Error concatenating dataframes") from e
    
    return df

def get_multiple_csv_objects_between_dates_threaded(
    bucket: str,
    prefix: str,
    start_date: datetime,
    end_date: datetime
) -> pd.DataFrame:
    """
    Retrieve multiple csv objects from an S3 bucket given a prefix and a
    date range, then concatenate them.
    """
    list_keys = list_objects_key_between_dates(bucket, prefix, start_date, end_date)
    if len(list_keys) == 0:
        raise ValueError(f"No data found between {start_date} and {end_date} at {prefix}")
    return get_multiple_csv_objects_threaded(bucket, list_keys)


def put_multiple_csv_objects_threaded(
    bucket: str,
    dfs: dict[str, pd.DataFrame],
    size_max_mb: float = 100,
    logger: logging.Logger | None = None
) -> None:
    """
    Put multiple csv objects into an S3 bucket given a folder.
    """
    with ThreadPoolExecutor() as executor:
        for prefix, df in dfs.items():
            if logger:
                logger.info(f"Putting object {prefix}")
            executor.submit(
                put_multiple_csv_objects, 
                bucket, 
                prefix, 
                df, 
                size_max_mb, 
            )
    if logger:
        logger.info("Upload finished")
    
