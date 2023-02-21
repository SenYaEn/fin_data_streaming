from kafka import KafkaConsumer
from json import dumps,loads
import json
from s3fs import S3FileSystem
from datetime import datetime
import pandas as pd

class ConsumerMarketstack:
    """Defines a Consumer object with Kafka topic as a source and S3 Bucket as a sink.
    
    Instance parameters:
    :kafka_topic: Kafka topic to which messages are to be posted
    :group_id_value: Group name to which Kafka topic belongs
    :auto_offset_reset_value: Kafka topic offset reset value
    :auto_commit_setting: Defines whether messages whould be commited as soon as received
    :s3_bucket_name: S3 bucket name where files should be saved
    :file_name: Fine name convention
    :kafka_server: Kafka bootstrap server
    """
    def __init__(self,
                 kafka_topic, 
                 group_id_value,
                 auto_offset_reset_value,
                 auto_commit_setting,
                 s3_bucket_name,
                 file_name,
                 kafka_server = "127.0.0.1:9092"):
        
        self.kafka_server = kafka_server
        self.kafka_topic = kafka_topic
        self.group_id_value = group_id_value
        self.auto_offset_reset_value = auto_offset_reset_value
        self.auto_commit_setting = auto_commit_setting
        self.s3_bucket_name = s3_bucket_name
        self.file_name = file_name
        
    def set_consumer(self):
        """Sets Kafka consumer object based on the provided parameters"""
        consumer = KafkaConsumer(
            self.kafka_topic, 
            value_deserializer = lambda x: json.loads(x) if x != "" else 0, 
            bootstrap_servers = self.kafka_server, 
            auto_offset_reset = self.auto_offset_reset_value,
            group_id = self.group_id_value,
            enable_auto_commit = self.auto_commit_setting)
        return consumer
    
    def main(self):
        """Executes full cycle data pull starting with consuming a message from 
        Kafka topic and ending with saving the message as a JSON file in S3 bucket"""
        s3 = S3FileSystem()
        consumer = self.set_consumer()
        
        try:  
            for c in consumer:    
                print(c.value)
                with s3.open(f"s3://{self.s3_bucket_name}/{self.file_name}".format(str(datetime.now().timestamp())), 'w') as file:
                    json.dump(c.value, file)
        except KeyboardInterrupt as kint:
            print("Stopping Consumer")
            print("Done Consuming")