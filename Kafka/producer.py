from kafka import KafkaProducer
from kafka import KafkaConsumer
import requests
import time
import json
from json import dumps
from s3fs import S3FileSystem
import datetime
import pandas as pd
import math

def get_intraday_api_response(base_url, path_params, limit, offset, symbols, access_key, date_from, interval):
    params = {
        'limit': limit,
        'offset': offset,
        'symbols': symbols,
        'access_key': access_key,
        'date_from': date_from,
        'interval': interval
    }
    api_result = requests.get(f"{base_url}/{path_params}", params)
    api_response = api_result.json()
    return api_response  

def produce(kafka_server, encoding, kafka_topic, message):
    producer = KafkaProducer(bootstrap_servers=[kafka_server],
                             value_serializer=lambda x: 
                             dumps(x).encode(encoding))
    producer.send(kafka_topic, value=message)
    
def get_latest_date_in_topic (kafka_topic, kafka_server, auto_offset_reset_value, consumer_timeout_ms_value):
    consumer = KafkaConsumer(
        kafka_topic, 
        value_deserializer=lambda x: json.loads(x) if x != "" else 0, 
        bootstrap_servers=kafka_server, 
        auto_offset_reset=auto_offset_reset_value,
        consumer_timeout_ms=consumer_timeout_ms_value)

    messages = []
    
    for item in consumer:
        messages.append(item.value)
    
    latest_date = pd.to_datetime(messages[len(messages)-1]["data"][0]["date"]).strftime("%Y-%m-%d %H:%M:%S")
    
    return latest_date



kafka_topic = "marketstack_intraday_apple"
kafka_server = '127.0.0.1:9092'
encoding = 'utf-8'
auto_offset_reset_value = "earliest"
consumer_timeout_ms_value = 2000
base_url = "https://api.marketstack.com/v1"
path_params = "intraday"
limit = 1000
symbols = "AAPL"
access_key = "47b5d7690a29320917634331aa6e0725"
interval = "1min"

date_from = get_latest_date_in_topic (kafka_topic, kafka_server, auto_offset_reset_value, consumer_timeout_ms_value)
# date_from = '2023-02-01 00:00:00'
    
try:
    while True:
        offset = 0
        initial_api_response = get_intraday_api_response(base_url, path_params, limit, offset, symbols, access_key, date_from, interval)
        limit = initial_api_response["pagination"]["limit"]
        total = initial_api_response["pagination"]["total"]
        total_to_limit_ratio = total/limit
        number_of_iterations = math.ceil(total_to_limit_ratio)
        last_date = pd.to_datetime(initial_api_response["data"][0]["date"]).strftime("%Y-%m-%d %H:%M:%S")
        
        if last_date == date_from:
            print("last_date: " + str(last_date) + " ; " + "date_from: " + str(date_from))
            print("No fresh data to load. Timestamp: " + str(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
            time.sleep(120)
        
        else:
            while number_of_iterations > 0:
                if number_of_iterations == 1:
                    offset = 0
                else:
                    offset = limit * (number_of_iterations-1)
                api_response = get_intraday_api_response(base_url, path_params, limit, offset, symbols, access_key, date_from, interval)
                produce(kafka_server, encoding, kafka_topic, api_response)
                print(api_response["pagination"])
                number_of_iterations = number_of_iterations - 1
                offset = offset - limit
                print(number_of_iterations)
                
            date_from = pd.to_datetime(initial_api_response["data"][0]["date"]).strftime("%Y-%m-%d %H:%M:%S")
            time.sleep(60)

except KeyboardInterrupt as kint:
    print("Stop producing")