import os
import json
import gzip
import time
import boto3
import logging
import datetime
import pandas as pd
from io import BytesIO
from botocore.exceptions import ClientError, BotoCoreError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

session = boto3.Session(region_name = os.environ['REGION'])

def read_secrets(secret_name):
    """
    Args:
        secret_name: Name of the secret in AWS Secrets Manager.
    Returns: JSON secret.
    """
    try:
        sm_client = session.client("secretsmanager")
        get_secret_value_response = sm_client.get_secret_value(
            SecretId=secret_name,
        )
        secret_str = get_secret_value_response["SecretString"]
        logger.info("secret retrieved successfully")

        secret = json.loads(secret_str)
    except ClientError as e:
        raise e

    return secret


class S3Manipulator:
    def __init__(self, session = None):
        """
        Initialize the S3Manipulator with a boto3 session.
        """
        self.session = boto3 if session is None else session        
        self.s3_client = self.session.client('s3')
        self.s3_resource = self.session.resource('s3')

    def upload_to_s3(self, file_content, bucket_name, file_key):
        """
        Upload files to a specific location in s3.
        """
        self.s3_client.put_object(Bucket=bucket_name, Key=file_key, Body=file_content)
        print(f"File uploaded to s3://{bucket_name}/{file_key}")

    def backup_files(self, s3_location, backup_bucket):
        """
        Backup files in a given path without changing the path of the folder.
        """
        source = s3_location.split('/', 1)
        try:
            for rptfiles in self.s3_resource.Bucket(source[0]).objects.filter(Prefix=source[1]).all():
                print(f"Copying file: {rptfiles.key} to backup bucket: {backup_bucket}")
                copy_source = {'Bucket': source[0], 'Key': rptfiles.key}
                self.s3_resource.Bucket(backup_bucket).copy(copy_source, rptfiles.key)

            print(f"Backup found at: s3://{backup_bucket}/{source[1]}")
        except (BotoCoreError, ClientError) as e:
            raise Exception(f"An error occurred: {e}")

    def get_all_dir(self, s3_location):
        """ 
        Returns all directories in a given path.
        """
        source = s3_location.split('/', 1)
        list_dir = set()
        try:
            for rptfiles in self.s3_resource.Bucket(source[0]).objects.filter(Prefix=source[1]).all():
                key = rptfiles.key.split('/')
                location = '/'.join(key[-4:-1])
                list_dir.add(location)

            print(f"Directories to process: {list_dir}")
            return list(list_dir)
        except (BotoCoreError, ClientError) as e:
            raise Exception(f"An error occurred: {e}")

    def read_s3_files(self, s3_location: str) -> pd.DataFrame:
        """
        Read all files in s3 and return the data as a DataFrame.
        Args:
            S3 URI 
        """
        source = s3_location.split('/', 1)
        df_list = []
        try:
            paginator = self.s3_client.get_paginator('list_objects_v2')
            print(f"Initiating process to read files from {s3_location}")
            for page in paginator.paginate(Bucket=source[0], Prefix=source[1]):
                if 'Contents' in page:
                    for obj in page['Contents']:
                        gz_data = self.s3_client.get_object(Bucket=source[0], Key=obj['Key'])['Body'].read()
                        with gzip.GzipFile(fileobj=BytesIO(gz_data)) as gzipfile:
                            json_data = [json.loads(line) for line in gzipfile]
                        df_list.append(pd.DataFrame(json_data))

            # Concatenate all DataFrames at once
            full_df = pd.concat(df_list, ignore_index=True) if df_list else pd.DataFrame()
            print(f"Files successfully read, Total rows read: {len(full_df)}")        
            return full_df

        except (BotoCoreError, ClientError) as e:
            print(f"Error reading S3 files: {e}")
            raise Exception(f"An error occurred: {e}")

    def delete_files(self, s3_location):
        """ 
        Deletes all the files in a given path.
        """
        source = s3_location.split('/', 1)
        try:
            for rptfiles in self.s3_resource.Bucket(source[0]).objects.filter(Prefix=source[1]).all():
                print(f"Deleting file: {rptfiles.key}")
                self.s3_resource.Object(source[0], rptfiles.key).delete()

            print(f"Deletion completed for prefix: {source[1]}")
        except (BotoCoreError, ClientError) as e:
            raise Exception(f"An error occurred: {e}")


class AthenaLoader:
    def __init__(self, config: dict, session = None) -> None:
        """
        Initialize the AthenaLoader with a boto3 session.
        """
        self.session = boto3 if session is None else session        
        self.athena_client = self.session.client("athena")
        self.s3_client = self.session.client("s3")
        """
        Initialize the AthenaLoader with a config.
        """
        self.query = config['query']
        self.query_results_bucket = config['query_results_bucket']
        self.results_location = config['results_location']
        self.query_id = None

    def run_athena_query(self):
        """ 
        Initiate query execution.
        """
        logger.info("commencing to run athena query:\n"+self.query.replace("\n",""))

        try:
            query_execution = self.athena_client.start_query_execution(
                QueryString = self.query,
                ResultConfiguration = {
                'OutputLocation': f"s3://{self.query_results_bucket}/{self.results_location}"
                }
            )
            self.query_id = query_execution['QueryExecutionId']
            logger.info(f"query started successfully:{self.query_id}")

        except Exception as err:
            print(f"error starting the query:{err}")
            raise err

    def monitor_query_status(self):
        """ 
        Retrieve query status. 
        """
        logger.info("query executions is being monitored")

        try:
            count_timeout = 0
            query_status = self.athena_client.get_query_execution(QueryExecutionId = self.query_id)
            while query_status['QueryExecution']['Status']['State'] != "SUCCEEDED":
                if query_status['QueryExecution']['Status']['State'] in ['FAILED', 'CANCELLED']:
                    print(f"Query failed with status: {query_status['QueryExecution']['Status']['State']} with {query_status['QueryExecution']['Status']['StateChangeReason']}")
                    raise query_status['QueryExecution']['Status']['State']
                else:
                    query_status = self.athena_client.get_query_execution(QueryExecutionId = self.query_id)
                    time.sleep(1)
                    count_timeout  += 1
                    if count_timeout  >= 60:
                        raise Exception('Athena query is taking longer than expected') # set timeout limit (60 sec by default)
            logger.info("query ended successfully")

        except Exception as err:
            print(f"error while executing the athena query:{err}")
            raise err

    def get_query_results(self):
        """ 
        Retrieve query results from Athena. 
        """
        logger.info("commencing to get query results")

        try:
            response = self.s3_client.get_object(Bucket = self.query_results_bucket, Key = self.results_location+self.query_id+'.csv')
            logger.info(f"query results available in: {self.query_results_bucket}/{self.results_location}{self.query_id}.csv")
            
            df = pd.DataFrame()
            df = pd.read_csv(BytesIO(response['Body'].read()), encoding = 'utf8', dtype=str, keep_default_na = False,na_values = [''])
            logger.info("results captured in df")

            return df
        except Exception as err:
            print(f"error while retrieving csv from s3:{err}")
            raise err
            
    def run_athena_loader(self):
        self.run_athena_query()
        self.monitor_query_status()
        df = self.get_query_results()
        logger.info(f"fields that are captured:{len(df)}")

        return df
    

class RedshiftQueryRunner:
    def __init__(self, config: dict, session = None) -> None:
        """
        Initialize the RedshiftQueryRunner with a boto3 session.
        """
        self.session = boto3 if session is None else session        
        self.redshift_client = self.session.client('redshift-data')
        self.s3_client = self.session.client("s3")
        """
        Initialize the RedshiftQueryRunner with a config.
        """
        self.query = config['query']
        self.cluster = config['cluster']
        self.database = config['database']
        self.redshift_role = config['redshift_role']
        self.output_location = config['output_location']
    
    def run_redshift_query(self):
        """Executes the Redshift query and waits for its completion."""

        # adding escape sequence to the original query
        query = self.query.replace("'", "''")
        create_query = f""" UNLOAD ('{query}') TO '{self.output_location}/qb_' iam_role '{self.redshift_role}' parallel off ALLOWOVERWRITE PARQUET; """
        try:
            res = self.redshift_client.execute_statement(
                Database=self.database,
                Sql=create_query,
                ClusterIdentifier=self.cluster
            )
            query_id = res['Id']
            logger.info(f"Redshift query execution initiated. Query ID: {query_id}")
            
            # Wait for query execution to complete
            def get_custom_waiter(self, client, query_id, timeout = 120):
                """Setup waiting object for most of the queries to Redshift"""
                start_time = time.time()  # Record the start time

                while True:
                    if time.time() - start_time > timeout:
                        logger.info("Timeout exceeded while waiting for query to complete.")
                        break

                    # Get the query status
                    response = client.describe_statement(Id=query_id)
                    status = response['Status']
                
                    if status == 'FINISHED':
                        print("Query has completed successfully.")
                        break
                    elif status == 'FAILED':
                        error_message = f"Cluster error: {response['Error']}"
                        raise Exception(error_message)
                    elif status == 'ABORTED':
                        print("Query was aborted.")
                        break
                    else:
                        print(f"Query is currently {status}. Waiting for next check...")
                        time.sleep(2)
            
            get_custom_waiter(self.redshift_client, query_id)
            logger.info("Redshift query execution completed.")

            # Check if files were created and create an empty one if not
            self.create_empty_file_if_none_exist()
            
            return query_id
        except Exception as e:
            print("Error occurred during query execution:")
            raise(e)
    
    def create_empty_file_if_none_exist(self):
        """Creates an empty Parquet file if no files exist in the output location."""
        
        # Parse the S3 bucket and prefix from the output location
        bucket_name = self.output_location.split('s3://')[1].split('/')[0]
        prefix = '/'.join(self.output_location.split('s3://')[1].split('/')[1:]) + '/qb_'
        
        # Check if any files exist with the given prefix
        response = self.s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
        
        if 'Contents' not in response:
            # No files were created, so create an empty Parquet file
            df = pd.DataFrame()
            df.to_parquet(f"{self.output_location}/qb_000.parquet")
            logger.info(f"Created empty Parquet file: s3://{bucket_name}/{prefix}000.parquet")
        else:
            print("Files already exist in the specified location.")
        
    def parquet_to_csv(self, query_id):
        """Converts parquet file to CSV format."""
        try:
            df = pd.read_parquet(f"{self.output_location}/qb_000.parquet")
            logger.info("Parquet file loaded successfully.")
            logger.info("Number of rows in DataFrame:", df.count())
            
            df.to_csv(f"{self.output_location}/{query_id}.csv", header='True', index=False)
            logger.info("Parquet file converted to CSV successfully.")
        except Exception as e:
            print("Error occurred during Parquet to CSV conversion:")
            raise(e)
    
    def delete_parquet(self):
        try:
            bucket_name, file_key= self.output_location[5:].split('/',1)
            file_key = f"{file_key}/qb_000.parquet"
            
            self.s3_client.delete_object(Bucket=bucket_name, Key=file_key)
            logger.info(f"Deleted Parquet file: s3://{bucket_name}/{file_key}")
        except Exception as e:
            error_message = f"Error deleting Parquet file: s3://{bucket_name}/{file_key}"
            print(error_message)
            raise(e)
        
    def run_redshift_query_runner(self):
        query_id = self.run_redshift_query()
        logger.info(f"Executing query with ID: {query_id}")
        
        self.parquet_to_csv(query_id)
        self.delete_parquet()
        return """Successfully executed the Redshift unload query, \\
            converted parquet to CSV, and deleted the parquet file."""


class LastrunTimeDynamoDB:
    def __init__(self, endpoint_url: str, check_table: str) -> None:
        self.endpoint_url = endpoint_url
        self.dynamo_table_name = check_table
        self.dynamo_table = None
        self.key_schema = [
            {
                'AttributeName': 'topic',
                'KeyType': 'HASH'
            }
        ]
        self.attribute_schema = [
            {
                'AttributeName': 'topic',
                'AttributeType': 'S'
            }
        ]
        self.init_table()

    def init_table(self) -> None:
        """ 
        Sets up connection and allocates handle to specific DynamoDB table,
        sets dynamo reference to Dynamo table that can be used for future calls. 
        """
        boto3_dynamo_db_resource = session.resource(
                'dynamodb',
                endpoint_url = self.endpoint_url,
                # region_name = aws_region
            )
        self.dynamo_table = boto3_dynamo_db_resource.Table(self.dynamo_table_name)
        try:
            self.dynamo_table.table_status in ("CREATING", "UPDATING", "DELETING", "ACTIVE")
            logger.info("DynamoDB table initiated")

        except ClientError as error:
            print('Error: Cannot find DynamoDB Table {}'.format(self.dynamo_table_name))
            print(error)
            self.dynamo_table = None

    def get(self, key: str):
        """ 
        Retrieve record from timestamp table for specific topic using DynamoDB API 
        """
        logger.info("commencing to retrieve timestamp from dynamoDB")
        try:
            response = self.dynamo_table.get_item(Key = {'topic': key})
        except ClientError as error:
            print('Error: Stopping because of {}'.format(error.response['Error']['Message']))
            raise error
                    
        else:
            logger.info("timestamp retrieved")
            return datetime.datetime.fromtimestamp(float(response.get('Item', {}).get('last_run_datetime', 0)))
        
    def set(self, topic: str, last_run_datetime: datetime.datetime):
        """ 
        Call DynamoDB api and put topic and unix timestamp 
        """
        logger.info("commencing to update dynamoDB table")
        try:
            self.dynamo_table.put_item(Item =
                {
                    'topic': topic,
                    'last_run_datetime': str(last_run_datetime)
                },
                ReturnValues = 'ALL_OLD'
            )
            logger.info("timestamp updated")
        except ClientError as error:
            print('Error: Cannot set item in DynamoDB Table {}, {}'.format(self.dynamo_table_name, error))
            raise error 
        else:
            return last_run_datetime

config = {}

al = AthenaLoader(config= {
    'query': config['query'],
    'query_results_bucket': config['query_results_bucket'],
    'results_location': config['results_location']
    })
al.run_athena_loader()

rqr = RedshiftQueryRunner(config ={
    'query': config['query'], 
    'database': config['DATABASE'], 
    'cluster': config['CLUSTER'], 
    'redshift_role': config['IAM_ROLE'], 
    'output_location': config['output_dynamic']
    })
rqr.run_redshift_query_runner()