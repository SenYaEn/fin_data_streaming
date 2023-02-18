from kafka import KafkaConsumer
from json import dumps,loads
import json
from s3fs import S3FileSystem
from datetime import datetime
import pandas as pd

class ConsumerMarketstack:
    
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
        consumer = KafkaConsumer(
            self.kafka_topic, 
            value_deserializer = lambda x: json.loads(x) if x != "" else 0, 
            bootstrap_servers = self.kafka_server, 
            auto_offset_reset = self.auto_offset_reset_value,
            group_id = self.group_id_value,
            enable_auto_commit = self.auto_commit_setting)
        return consumer
    
    def main(self):
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