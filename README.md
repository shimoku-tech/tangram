# Shimoku Tangram
A powerful Python library that simplifies data operations and file handling in S3, featuring smart compression, automatic threading for large datasets, and integrated logging.

## Key Features
- **Smart File Handling**: Automatic compression/decompression with gzip
- **Large Dataset Support**: Built-in threading for efficient processing of large files
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

### Basic Operations
~~~python
from shimoku_tangram.storage import s3
from shimoku_tangram.reporting.logging import init_logger

# Initialize logging
logger = init_logger("MyApp")

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

### Working with DataFrames
~~~python
import pandas as pd
from datetime import datetime, timedelta

# Store a large DataFrame (automatically chunks if needed)
df = pd.DataFrame(...)
s3.put_multiple_csv_objects(
    bucket="my-bucket",
    prefix="data/users",
    body=df,
    size_max_mb=100  # Chunks files larger than 100MB
)

# Read multiple CSV files as a single DataFrame
df_combined = s3.get_multiple_csv_objects(
    bucket="my-bucket",
    prefix="data/users"
)

# Process data by date range with automatic threading
start_date = datetime(2024, 1, 1)
end_date = datetime(2024, 3, 1)
df_quarterly = s3.get_multiple_csv_objects_between_dates_threaded(
    bucket="my-bucket",
    prefix="data/daily_metrics",
    start_date=start_date,
    end_date=end_date
)
~~~

### Batch Processing with Threading
~~~python
# Process multiple datasets in parallel
datasets = {
    "users/2024": users_df,
    "orders/2024": orders_df,
    "products/2024": products_df
}

s3.put_multiple_csv_objects_threaded(
    bucket="my-bucket",
    dfs=datasets,
    size_max_mb=50,
    logger=logger  # Optional progress logging
)
~~~

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

##### JSON
~~~python
# Store JSON with compression
put_json_object(bucket: str, key: str, body: dict, compress: bool = True) -> bool

# Read JSON (handles compression automatically)
get_json_object(bucket: str, key: str, compressed: bool = True) -> dict

# Store single JSON in a directory (clears existing)
put_single_json_object(bucket: str, prefix: str, body: Dict) -> str

# Read single JSON from a directory
get_single_json_object(bucket: str, prefix: str) -> dict
~~~

##### DataFrame Operations
~~~python
# Read multiple CSVs as single DataFrame
get_multiple_csv_objects(bucket: str, prefix: str) -> pd.DataFrame

# Store DataFrame as multiple CSVs if needed
put_multiple_csv_objects(
    bucket: str,
    prefix: str,
    body: pd.DataFrame,
    size_max_mb: float = 100
) -> List[str]

# Threaded operations for large datasets
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
~~~

##### Binary and Text
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

# Single object operations in directory
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

# Extract: Read last 7 days of data
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

# Load: Store results
s3.put_multiple_csv_objects(
    bucket="processed-data",
    prefix=f"weekly_summaries/{end_date.strftime('%Y-%m-%d')}",
    body=transformed_df
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

# Migrate in parallel
s3.put_multiple_csv_objects_threaded(
    bucket="new-bucket",
    dfs=dfs
)
~~~

## License
MIT
