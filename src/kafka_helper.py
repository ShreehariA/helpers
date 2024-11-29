import os
import ssl
import hvac
import json
import boto3
import logging
import requests
import pandas as pd
from time import sleep
from datetime import datetime
from kafka import KafkaProducer
from kafka import KafkaConsumer
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

session = boto3.Session(region_name = os.environ['REGION'])

def read_from_kafka(kakfka_topic_name, kafka_host):
    consumer = KafkaConsumer(
        kakfka_topic_name,
        bootstrap_servers=[kafka_host],security_protocol="SSL")
    
    count = 0
    for message in consumer:
        message = message.value
        count += 1
        print('{} received, running total: {}'.format(message, count))

class PushToKafka:
    def __init__(self, config: dict) -> None:
        self.vault_addr = config['vault_addr']
        self.common_name = config['common_name']
        self.vault_role_id = config['vault_role_id']
        self.vault_secret_id = config['vault_secret_id']
        self.vault_secret_path = config['vault_secret_path']
        self.kafka_host = config['kafka_host']

    def get_vault_app_role(self, role_id, secret_id):
        """
        Retrieve Vault app role and secret ID from AWS SSM.
        """
        try:
            logger.info("Fetching Vault app role and secret ID.")
            ssm = session.client("ssm", region_name="us-east-2")
            print("conncetion to ssm compelte")
            vault_app_role_id = ssm.get_parameter(Name=role_id, WithDecryption=True)[
                "Parameter"
            ]["Value"]
            vault_secret_id = ssm.get_parameter(Name=secret_id, WithDecryption=True)[
                "Parameter"
            ]["Value"]
            logger.info("Successfully fetched Vault credentials.")
            return vault_app_role_id, vault_secret_id
        except ClientError as e:
            print(f"Error retrieving Vault credentials: {e}")
            raise e

    def revoke_token(self, token):
        """
        Revoke the Vault token.
        """
        try:
            revoke_path = "auth/token/revoke-self"
            # revoke_path = secret["revoke_token_path"]
            url = f"{self.vault_addr}/v1/{revoke_path}"
            headers = {
                "X-Vault-Token": token,
                "Content-Type": "application/json",
            }
            response = requests.post(url, headers=headers, data=json.dumps({}))
            if response.ok:
                logger.info("Vault token revoked successfully.")
            else:
                logger.error(f"Failed to revoke Vault token. Status code: {response.status_code}")
        except Exception as e:
            print(f"Error revoking Vault token: {e}")
            raise e

    def get_kafka_cert(self, vault_secret_path):
        """
        Retrieve Kafka certificate from Vault and save it locally.
        """
        try:
            logger.info("Retrieving Kafka certificate from Vault.")
            vault_app_role_id, vault_secret_id = self.get_vault_app_role(
                self.vault_role_id, self.vault_secret_id
            )
            client = hvac.Client(self.vault_addr)
            resp = client.auth.approle.login(
                role_id=vault_app_role_id,
                secret_id=vault_secret_id,
            )
            if not client.is_authenticated():
                logger.error("Failed to authenticate with Vault.")
                raise ValueError("Vault authentication failed.")
            token = resp["auth"]["client_token"]
            headers = {
                "X-Vault-Token": token,
                "Content-Type": "application/json",
            }
            url = f"{self.vault_addr}/v1/{vault_secret_path}"
            payload = json.dumps({"common_name": self.common_name})

            response = requests.request("POST", url, headers=headers, data=payload)
            # print(response.json()["data"])
            self.revoke_token(token)

            ca_chain = response.json()["data"]["ca_chain"]
            certificate = response.json()["data"]["certificate"]
            private_key = response.json()["data"]["private_key"]
            # ca = response.json()["data"]["issuing_ca"]

            with open("/tmp/key.pem", "w") as pem_file:
                pem_file.write(private_key)
            with open("/tmp/certificate.pem", "w") as cert_file:
                cert_file.write(certificate)
            with open("/tmp/CARoot.pem", "w") as ca_file:
                ca_file.write("\n".join(ca_chain))

            logger.info("Kafka certificate retrieved and saved successfully.")
        except Exception as e:
            print(f"Error retrieving Kafka certificate: {e}")
            raise e

    def kafka_connect(self, host):
        """
        Connect to Kafka using SSL.
        """
        try:
            logger.info("Connecting to Kafka topic.")
            context = ssl.create_default_context()
            context.check_hostname = False
            context.verify_mode = ssl.CERT_NONE
            context.load_cert_chain(certfile="/tmp/certificate.pem", keyfile="/tmp/key.pem")
            context.load_verify_locations(cafile="/tmp/CARoot.pem")

            # print(context)
            logger.info("connecting...")
            producer = KafkaProducer(
                bootstrap_servers=[host], 
                security_protocol="SSL", 
                ssl_context=context
            )
            # print(producer)
            logger.info("Connected to Kafka.")
            return producer
        except Exception as e:
            print(f"Error connecting to Kafka: {e}")
            raise e
    
    def stream_df_to_topic(self, df, topic_name, kafka_producer, batch_size=1000):
        """
        Stream a DataFrame to a Kafka topic efficiently, compatible with AWS Lambda and Glue.

        Args:
            df: Pandas DataFrame
            kafka_producer: Kafka producer object
            topic_name: Kafka topic name
            batch_size: Number of records to send in each batch

        Returns: None
        """
        # df['type'] = "type"
        df.fillna("")
        records = df.to_dict('records')
        total_records = len(records)
        
        def send_batch(batch):
            """    
            Send a batch of records to a Kafka topic.
            """
            try:
                for record in batch:
                    json_data = json.dumps(record)
                    key = bytes(str(record.get('id', '')), "utf-8")
                    kafka_producer.send(
                        topic_name,
                        key=key,
                        value=bytes(json_data, "utf-8")
                    )
            except Exception as e:
                print(f"Error sending record to Kafka: {e}\nrecord {record}")
                raise e
            
            try:  
                kafka_producer.flush()
            except Exception as e:
                print(f"Error flushing kafka producer: {e}")
                raise e
            

        for i in range(0, total_records, batch_size):
            batch = records[i:i+batch_size]
            try:
                send_batch(batch)
            except Exception as e:
                logger.error(f"Error processing batch starting at index {i}: {e}")
                raise e

            if i + batch_size < total_records:  # Don't sleep after the last batch
                sleep(0.1)  # Small delay to prevent overwhelming the Kafka broker

        logger.info(f"Pushed {total_records} records to Kafka topic: {topic_name}")

    def run_push_to_kafka(self, df, topic_name: str):
        self.get_kafka_cert(self.vault_secret_path)
        kafka_producer = self.kafka_connect(self.kafka_host)
        self.stream_df_to_topic(df, topic_name, kafka_producer)
        kafka_producer.close()
        logger.info("Kafka producer closed.")

# Example usage
df = pd.DataFrame()
topic_name = "test_topic"
config = {}
    
# Create instance to push to kafka
ptk = PushToKafka(config={
    'vault_role_id': config['vault_role_id'],
    'vault_secret_id': config['vault_secret_id'],
    'vault_addr': config['VAULT_ADDR'],
    'common_name': config['common_name'],
    'vault_secret_path': config['vault_secret_path'],
    'kafka_host': config['kafka_host']
})
# using the instance to push
ptk.run_push_to_kafka(df, topic_name)

# need to test once
class KafkaDataFrameConsumer:
    """
    A class to consume messages from Kafka and convert them to pandas DataFrames.
    """
    
    def __init__(
        self,
        topic_name: str,
        bootstrap_servers: str,
        security_protocol: str = "SSL",
        # group_id: Optional[str] = None,
        auto_offset_reset: str = 'latest',
        enable_auto_commit: bool = True
    ):
        """
        Initialize the Kafka consumer with the given configuration.
        
        Args:
            topic_name: Name of the Kafka topic to consume from
            bootstrap_servers: Kafka broker address(es)
            security_protocol: Security protocol to use (default: SSL)
            group_id: Consumer group ID (optional)
            auto_offset_reset: Where to start reading messages ('latest' or 'earliest')
            enable_auto_commit: Whether to auto-commit offsets
        """
        self.topic_name = topic_name
        self.bootstrap_servers = bootstrap_servers
        self.security_protocol = security_protocol
        # self.group_id = group_id
        self.auto_offset_reset = auto_offset_reset
        self.enable_auto_commit = enable_auto_commit
        self.consumer = None
        self.message_count = 0
        
    def _initialize_consumer(self, timeout_ms: int) -> None:
        """Initialize the Kafka consumer with the configured parameters."""
        self.consumer = KafkaConsumer(
            self.topic_name,
            bootstrap_servers=[self.bootstrap_servers],
            security_protocol=self.security_protocol,
            # group_id=self.group_id,
            auto_offset_reset=self.auto_offset_reset,
            enable_auto_commit=self.enable_auto_commit,
            consumer_timeout_ms=timeout_ms
        )
    
    def _process_message(self, message) -> dict:
        """Process a single Kafka message and return it as a dictionary."""
        if isinstance(message.value, bytes):
            data = json.loads(message.value.decode('utf-8'))
        else:
            data = message.value
            
        # Add metadata
        data.update({
            'kafka_timestamp': datetime.fromtimestamp(message.timestamp / 1000.0),
            'kafka_partition': message.partition,
            'kafka_offset': message.offset
        })
        
        return data
    
    def read_messages(
        self,
        max_messages: int = None,
        timeout_ms: int = 10000,
        include_metadata: bool = True
    ) -> pd.DataFrame:
        """
        Read messages from Kafka and return them as a pandas DataFrame.
        
        Args:
            max_messages: Maximum number of messages to read (optional)
            timeout_ms: Consumer timeout in milliseconds
            include_metadata: Whether to include Kafka metadata in the DataFrame
            
        Returns:
            DataFrame containing the messages
        """
        messages = []
        self.message_count = 0
        
        try:
            self._initialize_consumer(timeout_ms)
            
            for message in self.consumer:
                try:
                    data = self._process_message(message)
                    messages.append(data)
                    self.message_count += 1
                    
                    if self.message_count % 1000 == 0:
                        print(f'Processed {self.message_count} messages')
                    
                    if max_messages and self.message_count >= max_messages:
                        break
                        
                except json.JSONDecodeError as e:
                    print(f"Error decoding message: {e}")
                    continue
                except Exception as e:
                    print(f"Error processing message: {e}")
                    continue
                    
        except Exception as e:
            print(f"Error reading from Kafka: {e}")
        finally:
            self.close()
        
        if not messages:
            return pd.DataFrame()
        
        df = pd.DataFrame(messages)
        
        # Remove metadata columns if not requested
        if not include_metadata:
            metadata_cols = ['kafka_timestamp', 'kafka_partition', 'kafka_offset']
            df = df.drop(columns=[col for col in metadata_cols if col in df.columns])
        
        print(f"\nTotal messages processed: {self.message_count}")
        print(f"DataFrame shape: {df.shape}")
        
        return df
    
    def close(self) -> None:
        """Close the Kafka consumer connection."""
        if self.consumer:
            self.consumer.close()
            self.consumer = None
    
    def __enter__(self):
        """Enable use with context manager (with statement)."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Ensure consumer is closed when exiting context manager."""
        self.close()

# Example usage:
if __name__ == "__main__":
    # Create consumer instance
    consumer = KafkaDataFrameConsumer(
        topic_name="my-topic",
        bootstrap_servers="localhost:9092",
        group_id="my-group",
        auto_offset_reset="earliest"
    )
    
    # Method 1: Direct usage
    df = consumer.read_messages(max_messages=1000)
    consumer.close()
    
    # Method 2: Using context manager
    with KafkaDataFrameConsumer(
        topic_name="my-topic",
        bootstrap_servers="localhost:9092"
    ) as consumer:
        df = consumer.read_messages(max_messages=1000)