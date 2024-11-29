# helpers

A place for all my helper class and functions.

## Description

This repository contains various helper classes and functions to simplify common tasks and improve code reusability. The helpers are organized into different modules based on their functionality.

## Installation

To use the helper classes and functions, you need to have Python installed on your system. You can install the required dependencies using `pip`:

```bash
pip install -r requirements.txt
```

## Usage

To use the helper classes and functions, you need to import them into your Python script. Here are some examples:

### Importing a helper class

```python
from src.aws_helper import S3Manipulator

s3_manipulator = S3Manipulator()
s3_manipulator.upload_to_s3(file_content, bucket_name, file_key)
```

### Importing a helper function

```python
from src.api_helper import sat_auth

sat_token = sat_auth(sat_oauth_link, secrets)
```

## Examples

Here are some examples demonstrating how to use the helper classes and functions:

### Example 1: Using the S3Manipulator class

```python
from src.aws_helper import S3Manipulator

s3_manipulator = S3Manipulator()
s3_manipulator.upload_to_s3(file_content, bucket_name, file_key)
```

### Example 2: Using the sat_auth function

```python
from src.api_helper import sat_auth

sat_token = sat_auth(sat_oauth_link, secrets)
```

## Available Helper Classes and Functions

### `src/api_helper.py`

- `sat_auth(sat_oauth_link: str, secrets: dict, timeout: int = 900) -> str`: Get auth token from SAT.
- `call_lookup_api(url: str, user_guid: str, sat_token: str) -> pd.DataFrame`: Calls the lookup API using the given URL, user GUID, and SAT token, and returns a DataFrame of the results.

### `src/aws_helper.py`

- `read_secrets(secret_name)`: Retrieve secret from AWS Secrets Manager.
- `S3Manipulator`: A class to manipulate S3 objects.
  - `upload_to_s3(file_content, bucket_name, file_key)`: Upload files to a specific location in S3.
  - `backup_files(s3_location, backup_bucket)`: Backup files in a given path without changing the path of the folder.
  - `get_all_dir(s3_location)`: Returns all directories in a given path.
  - `read_s3_files(s3_location: str) -> pd.DataFrame`: Read all files in S3 and return the data as a DataFrame.
  - `delete_files(s3_location)`: Deletes all the files in a given path.
- `AthenaLoader`: A class to load data from Athena.
  - `run_athena_query()`: Initiate query execution.
  - `monitor_query_status()`: Retrieve query status.
  - `get_query_results()`: Retrieve query results from Athena.
  - `run_athena_loader()`: Run the Athena loader.
- `RedshiftQueryRunner`: A class to run queries on Redshift.
  - `run_redshift_query()`: Executes the Redshift query and waits for its completion.
  - `create_empty_file_if_none_exist()`: Creates an empty Parquet file if no files exist in the output location.
  - `parquet_to_csv(query_id)`: Converts Parquet file to CSV format.
  - `delete_parquet()`: Deletes the Parquet file.
  - `run_redshift_query_runner()`: Run the Redshift query runner.
- `LastrunTimeDynamoDB`: A class to manage last run time in DynamoDB.
  - `init_table()`: Initialize the DynamoDB table.
  - `get(key: str)`: Retrieve record from timestamp table for specific topic using DynamoDB API.
  - `set(topic: str, last_run_datetime: datetime.datetime)`: Call DynamoDB API and put topic and Unix timestamp.

### `src/kafka_helper.py`

- `read_from_kafka(kakfka_topic_name, kafka_host)`: Read messages from a Kafka topic.
- `PushToKafka`: A class to push data to Kafka.
  - `get_vault_app_role(role_id, secret_id)`: Retrieve Vault app role and secret ID from AWS SSM.
  - `revoke_token(token)`: Revoke the Vault token.
  - `get_kafka_cert(vault_secret_path)`: Retrieve Kafka certificate from Vault and save it locally.
  - `kafka_connect(host)`: Connect to Kafka using SSL.
  - `stream_df_to_topic(df, topic_name, kafka_producer, batch_size=1000)`: Stream a DataFrame to a Kafka topic efficiently.
  - `run_push_to_kafka(df, topic_name: str)`: Run the push to Kafka process.
- `KafkaDataFrameConsumer`: A class to consume messages from Kafka and convert them to pandas DataFrames.
  - `read_messages(max_messages: int = None, timeout_ms: int = 10000, include_metadata: bool = True) -> pd.DataFrame`: Read messages from Kafka and return them as a pandas DataFrame.
  - `close()`: Close the Kafka consumer connection.

### `src/slack_helper.py`

- `slack_send_files(file_content, slack_token: str, channel_id: str) -> None`: Sends a notification to a Slack channel.
- `slack_send_table(secret, token)`: Sends a table to a Slack channel.
