from kafka import KafkaConsumer
from time import sleep
from json import dumps,loads
import json
from s3fs import S3FileSystem
from datetime import datetime


def consume(kafka_server, kafka_topic, auto_offset_reset_value, s3_bucket_name, file_name):
    
    s3 = S3FileSystem()
    
    consumer = KafkaConsumer(
        kafka_topic, 
        value_deserializer=lambda x: json.loads(x) if x != "" else 0, 
        bootstrap_servers=kafka_server, 
        auto_offset_reset=auto_offset_reset_value)
    
    try:  
        for c in consumer:    
            print(c.value)
            with s3.open(f"s3://{s3_bucket_name}/{file_name}".format(str(datetime.now().timestamp())), 'w') as file:
                json.dump(c.value, file)
    except KeyboardInterrupt as kint:
        print("Stopping Consumer")
        print("Done Consuming")


kafka_server = "127.0.0.1:9092"
kafka_topic = "test_stocks"
auto_offset_reset_value = "latest"
s3_bucket_name = "kafka-test-senyaen"
file_name = "stocks_test_{}.json"

consume(kafka_server, kafka_topic, auto_offset_reset_value, s3_bucket_name, file_name)