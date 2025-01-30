# Shimoku Tangram

A Python library for data handling, storage operations, and logging.

## Installation

~~~bash
pip install shimoku-tangram
~~~

## Quick Start

~~~python
from shimoku_tangram.storage import s3
from shimoku_tangram.reporting.logging import init_logger

# Initialize logging
logger = init_logger("MyLogger")

# Use S3 operations
s3.put_json_object("my-bucket", "path/to/file.json", {"key": "value"})
~~~

## Components

### Storage (s3)

All available methods in the S3 module:

#### Basic Bucket Operations
~~~python
bucket_exists(bucket: str) -> bool
clear_path(bucket: str, prefix: str = "") -> bool
~~~

#### Object Listing
~~~python
list_objects_metadata(bucket: str, prefix: str = "") -> list
list_objects_key(bucket: str, prefix: str = "") -> list
list_single_object_key(bucket: str, prefix: str) -> str
list_multiple_objects_keys(bucket: str, prefix: str) -> List[str]
list_objects_key_between_dates(bucket: str, prefix: str, start_date: datetime, end_date: datetime) -> List[str]
~~~

#### Basic Object Operations
~~~python
get_object(bucket: str, key: str, compressed: bool = True) -> bytes
put_object(bucket: str, key: str, body: bytes, compress: bool = True) -> bool
delete_object(bucket: str, key: str) -> bool
~~~

#### Text Operations
~~~python
get_text_object(bucket: str, key: str, encoding: str = "utf-8", compressed: bool = True) -> str
put_text_object(bucket: str, key: str, body: str, encoding: str = "utf-8", compress: bool = True) -> bool
~~~

#### JSON Operations
~~~python
get_json_object(bucket: str, key: str, compressed: bool = True) -> dict
put_json_object(bucket: str, key: str, body: dict, compress: bool = True) -> bool
get_single_json_object(bucket: str, prefix: str) -> dict
put_single_json_object(bucket: str, prefix: str, body: Dict) -> str
~~~

#### Pickle Operations
~~~python
get_pkl_object(bucket: str, key: str, compressed: bool = True) -> dict
put_pkl_object(bucket: str, key: str, body, compress: bool = True) -> bool
get_single_pkl_object(bucket: str, prefix: str)
put_single_pkl_object(bucket: str, prefix: str, body) -> str
~~~

#### CSV/DataFrame Operations
~~~python
get_multiple_csv_objects(bucket: str, prefix: str) -> pd.DataFrame
put_multiple_csv_objects(bucket: str, prefix: str, body: pd.DataFrame, size_max_mb: float = 100) -> List[str]
~~~

#### Threaded Operations
~~~python
get_multiple_csv_objects_threaded(bucket: str, prefixes: list[str], logger: logging.Logger | None = None) -> pd.DataFrame
put_multiple_csv_objects_threaded(bucket: str, dfs: dict[str, pd.DataFrame], size_max_mb: float = 100, logger: logging.Logger | None = None) -> None
get_multiple_csv_objects_between_dates_threaded(bucket: str, prefix: str, start_date: datetime, end_date: datetime) -> pd.DataFrame
~~~

#### Utility Functions
~~~python
get_extension(key: str, compressed: bool = True) -> str
is_compressed(key: str) -> bool
~~~

#### Metadata
~~~python
get_last_timestamp(bucket: str, prefix: str) -> str
set_last_timestamp(bucket: str, prefix: str) -> str
~~~

### Logging

Initialize with custom name and level:

~~~python
from shimoku_tangram.reporting.logging import init_logger
logger = init_logger("MyLogger", logging.INFO)
~~~

## Features

- Automatic compression (gzip)
- Thread support for large operations
- Pandas DataFrame integration
- S3 path management
- Structured logging
- Error handling

## Example

~~~python
from shimoku_tangram.storage import s3
from datetime import datetime

# Get data between dates
df = s3.get_multiple_csv_objects_between_dates_threaded(
    bucket="my-bucket",
    prefix="data/path",
    start_date=datetime(2024, 1, 1),
    end_date=datetime(2024, 12, 31)
)

# Store processed data
s3.put_multiple_csv_objects(
    bucket="my-bucket",
    prefix="output/path",
    body=df,
    size_max_mb=50
)
~~~

## License

MIT
