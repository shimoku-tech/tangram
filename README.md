# Shimoku Tangram
A powerful Python library that simplifies data operations and file handling in S3, featuring automatic threading for high-performance data processing, smart compression, and integrated logging.

## Index
1. [Key Features](#key-features)
2. [Installation](#installation)
3. [Quick Start Guide](#quick-start-guide)
4. [High-Performance Operations](#high-performance-operations)
5. [API Reference](#api-reference)
6. [Common Use Cases](#common-use-cases)
7. [Meta S3 Usage](#meta-s3-usage)
8. [License](#license)

## Key Features
- **High Performance**: Built-in threading for parallel processing of large datasets
- **Smart File Handling**: Automatic compression/decompression with gzip
- **Pandas Integration**: Direct DataFrame reading/writing with automatic chunking
- **Date-based Operations**: Filter and process files by date ranges
- **Flexible Formats**: Support for JSON, CSV, Pickle, and raw text/binary data
- **Path Management**: Utilities for S3 path handling and object listing
- **Structured Logging**: Integrated logging system with configurable levels

## Installation
~~~bash
pip install shimoku-tangram
~~~

## Quick Start Guide

### Recommended Pattern: High-Performance Data Operations
~~~python
from shimoku_tangram.storage import s3
from shimoku_tangram.reporting.logging import init_logger
from datetime import datetime

# Initialize logging
logger = init_logger("MyApp")

# Read multiple CSVs with parallel processing
df = s3.get_multiple_csv_objects_threaded(
    bucket="my-bucket",
    prefixes=["data/2024/01", "data/2024/02"],
    logger=logger  # Track progress
)

# Process data by date range efficiently
df_range = s3.get_multiple_csv_objects_between_dates_threaded(
    bucket="my-bucket",
    prefix="daily_metrics",
    start_date=datetime(2024, 1, 1),
    end_date=datetime(2024, 3, 1)
)

# Write multiple DataFrames in parallel
datasets = {
    "users/2024": users_df,
    "orders/2024": orders_df,
    "products/2024": products_df
}
s3.put_multiple_csv_objects_threaded(
    bucket="my-bucket",
    dfs=datasets,
    size_max_mb=50,
    logger=logger
)
~~~

### Basic Operations
~~~python
# Store JSON data with automatic compression
s3.put_json_object(
    bucket="my-bucket",
    key="config/settings.json",
    body={"api_key": "abc123", "max_retries": 3}
)

# Read JSON data (automatically handles compression)
config = s3.get_json_object(
    bucket="my-bucket",
    key="config/settings.json"
)
~~~

## High-Performance Operations

### Threaded CSV Operations (Recommended)
These methods provide optimal performance for data operations:

~~~python
# Parallel read of multiple CSV files (5-10x faster than non-threaded)
df = s3.get_multiple_csv_objects_threaded(
    bucket="my-bucket",
    prefixes=["data/2024/01", "data/2024/02"],
    logger=logger  # Optional progress tracking
)

# Parallel write of multiple DataFrames (3-7x faster than non-threaded)
s3.put_multiple_csv_objects_threaded(
    bucket="my-bucket",
    dfs={"path1": df1, "path2": df2},
    size_max_mb=50
)

# Date-based parallel processing (Automatically handles date directories)
df = s3.get_multiple_csv_objects_between_dates_threaded(
    bucket="my-bucket",
    prefix="daily_data",
    start_date=datetime(2024, 1, 1),
    end_date=datetime(2024, 3, 31)
)
~~~

Performance Comparison:
- Reading 100 CSV files:
  - Non-threaded: ~10 seconds
  - Threaded: ~2 seconds
- Writing 50 DataFrames:
  - Non-threaded: ~25 seconds
  - Threaded: ~5 seconds

## API Reference

### Storage Operations

#### Bucket Management
~~~python
# Check bucket existence
bucket_exists(bucket: str) -> bool

# Clear all objects under a prefix
clear_path(bucket: str, prefix: str = "") -> bool
~~~

#### Object Listing
~~~python
# List objects with metadata
list_objects_metadata(bucket: str, prefix: str = "") -> list

# List object keys
list_objects_key(bucket: str, prefix: str = "") -> list

# Get single object key (fails if multiple found)
list_single_object_key(bucket: str, prefix: str) -> str

# List multiple object keys with pagination
list_multiple_objects_keys(bucket: str, prefix: str) -> List[str]

# List objects within a date range
list_objects_key_between_dates(
    bucket: str,
    prefix: str, 
    start_date: datetime, 
    end_date: datetime
) -> List[str]
~~~

#### Data Operations

##### DataFrame Operations (CSV)
~~~python
# RECOMMENDED: Threaded operations for optimal performance
get_multiple_csv_objects_threaded(
    bucket: str,
    prefixes: list[str],
    logger: logging.Logger | None = None
) -> pd.DataFrame

put_multiple_csv_objects_threaded(
    bucket: str,
    dfs: dict[str, pd.DataFrame],
    size_max_mb: float = 100,
    logger: logging.Logger | None = None
) -> None

get_multiple_csv_objects_between_dates_threaded(
    bucket: str,
    prefix: str,
    start_date: datetime,
    end_date: datetime
) -> pd.DataFrame

# Standard operations (use threaded versions for better performance)
get_multiple_csv_objects(bucket: str, prefix: str) -> pd.DataFrame
put_multiple_csv_objects(
    bucket: str,
    prefix: str,
    body: pd.DataFrame,
    size_max_mb: float = 100
) -> List[str]
~~~

##### JSON Operations
~~~python
# Store JSON with compression
put_json_object(bucket: str, key: str, body: dict, compress: bool = True) -> bool

# Read JSON (handles compression automatically)
get_json_object(bucket: str, key: str, compressed: bool = True) -> dict

# Single file operations
put_single_json_object(bucket: str, prefix: str, body: Dict) -> str
get_single_json_object(bucket: str, prefix: str) -> dict
~~~

##### Binary and Text Operations
~~~python
# Binary operations
get_object(bucket: str, key: str, compressed: bool = True) -> bytes
put_object(bucket: str, key: str, body: bytes, compress: bool = True) -> bool

# Text operations with encoding support
get_text_object(
    bucket: str,
    key: str,
    encoding: str = "utf-8",
    compressed: bool = True
) -> str

put_text_object(
    bucket: str,
    key: str,
    body: str,
    encoding: str = "utf-8",
    compress: bool = True
) -> bool
~~~

##### Pickle Operations
~~~python
# Store Python objects
put_pkl_object(bucket: str, key: str, body, compress: bool = True) -> bool
get_pkl_object(bucket: str, key: str, compressed: bool = True) -> dict

# Single object operations
put_single_pkl_object(bucket: str, prefix: str, body) -> str
get_single_pkl_object(bucket: str, prefix: str)
~~~

### Logging
Initialize logging with custom configuration:
~~~python
from shimoku_tangram.reporting.logging import init_logger

# Basic initialization
logger = init_logger("MyApp")

# Custom level
logger = init_logger("MyApp", logging.DEBUG)

# Use in your code
logger.info("Processing started")
logger.debug("Detailed information")
logger.warning("Warning message")
~~~

## Common Use Cases

### ETL Pipeline
~~~python
from shimoku_tangram.storage import s3
from datetime import datetime, timedelta

# Extract: Read last 7 days of data efficiently
end_date = datetime.now()
start_date = end_date - timedelta(days=7)
df = s3.get_multiple_csv_objects_between_dates_threaded(
    bucket="raw-data",
    prefix="daily_events",
    start_date=start_date,
    end_date=end_date
)

# Transform: Process data
transformed_df = process_data(df)

# Load: Store results efficiently
s3.put_multiple_csv_objects_threaded(
    bucket="processed-data",
    dfs={f"weekly_summaries/{end_date.strftime('%Y-%m-%d')}": transformed_df}
)
~~~

### Data Migration
~~~python
# List all objects to migrate
keys = s3.list_multiple_objects_keys(
    bucket="old-bucket",
    prefix="data/2023"
)

# Prepare datasets
dfs = {}
for key in keys:
    df = s3.get_multiple_csv_objects("old-bucket", key)
    new_key = f"migrated/{key}"
    dfs[new_key] = df

# Migrate in parallel for optimal performance
s3.put_multiple_csv_objects_threaded(
    bucket="new-bucket",
    dfs=dfs
)
~~~

## Meta S3 Usage
The `meta_s3` module provides utilities for managing metadata in S3, such as tracking the last timestamp of operations. This is particularly useful for ETL pipelines where you need to keep track of the latest processed data.

### Example: Tracking Last Timestamp
~~~python
from datetime import datetime
from shimoku_tangram.storage import meta_s3

# Get the last timestamp from S3 metadata
last_timestamp = meta_s3.get_last_timestamp(bucket="my-bucket", prefix="data/raw")

# Set the last timestamp to the current time
new_timestamp = meta_s3.set_last_timestamp(bucket="my-bucket", prefix="data/raw")
~~~

### Use Case in ETL Pipelines
~~~python
# During data extraction
last_timestamp = meta_s3.get_last_timestamp(bucket="my-bucket", prefix="data/raw")
if last_timestamp:
    # Process only new data since the last timestamp
    df = s3.get_multiple_csv_objects_between_dates_threaded(
        bucket="my-bucket",
        prefix="data/raw",
        start_date=last_timestamp,
        end_date=datetime.now()
    )

# After processing, update the last timestamp
meta_s3.set_last_timestamp(bucket="my-bucket", prefix="data/raw")
~~~

### Methods
- **`get_last_timestamp(bucket: str, prefix: str) -> str`**: Retrieves the last timestamp stored in S3 metadata.
- **`set_last_timestamp(bucket: str, prefix: str) -> str`**: Updates the last timestamp in S3 metadata to the current time.

## License
MIT
