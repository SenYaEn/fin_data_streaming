from kafka import KafkaConsumer
from json import dumps,loads
import json
from s3fs import S3FileSystem
from datetime import datetime
import pandas as pd

def consume (kafka_topic, kafka_server, auto_offset_reset_value, group_id_value, auto_commit_setting, s3_bucket_name, file_name):
    
    s3 = S3FileSystem()

    consumer = KafkaConsumer(
        kafka_topic, 
        value_deserializer=lambda x: json.loads(x) if x != "" else 0, 
        bootstrap_servers=kafka_server, 
        auto_offset_reset=auto_offset_reset_value,
        group_id = group_id_value,
        enable_auto_commit=auto_commit_setting)
    
    try:  
        for c in consumer:    
            print(c.value)
            with s3.open(f"s3://{s3_bucket_name}/{file_name}".format(str(datetime.now().timestamp())), 'w') as file:
                json.dump(c.value, file)
    except KeyboardInterrupt as kint:
        print("Stopping Consumer")
        print("Done Consuming")



kafka_server = "127.0.0.1:9092"
kafka_topic = "marketstack_intraday_apple"
group_id_value = "marketstack_intraday_apple_group"
auto_offset_reset_value = "earliest"
auto_commit_setting = True
s3_bucket_name = "marketstack-intraday"
file_name = "intraday_apple{}.json"

consume (kafka_topic, kafka_server, auto_offset_reset_value, group_id_value, auto_commit_setting, s3_bucket_name, file_name)