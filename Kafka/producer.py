import pandas as pd
import time
import json
from kafka import KafkaProducer
from json import dumps

def produce(kafka_topic, kafka_server, encoding, df):
    
    producer = KafkaProducer(bootstrap_servers=[kafka_server],
                             value_serializer=lambda x: 
                             dumps(x).encode(encoding))
    
    try:
        while True:
            message = df.sample(1).to_dict(orient="records")
            producer.send(kafka_topic, value=message)
            print(message)
            time.sleep(1)
    except KeyboardInterrupt as kint:
        print("Stop producing")


kafka_topic = "test_stocks"
kafka_server = '127.0.0.1:9092'
encoding = 'utf-8'
df = pd. read_csv("C:/Users/User/OneDrive/Study/Kafka_Projects/data/export.csv")

produce(kafka_topic, kafka_server, encoding, df)