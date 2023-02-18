from kafka import KafkaProducer
from kafka import KafkaConsumer
import requests
import time
import json
from json import dumps
import datetime
import pandas as pd
import math

class ProducerMarketstack:
    base_url = "https://api.marketstack.com/v1"
    encoding = 'utf-8'
    path_params = "intraday"
    limit = 1000
    
    def __init__(self, 
                 kafka_topic, 
                 kafka_server, 
                 symbols, 
                 access_key, 
                 interval,
                 auto_offset_reset_value = "earliest", 
                 consumer_timeout_ms_value = 2000):
        
        self.kafka_topic = kafka_topic
        self.kafka_server = kafka_server
        self.auto_offset_reset_value = auto_offset_reset_value
        self.consumer_timeout_ms_value = consumer_timeout_ms_value
        self.symbols = symbols
        self.access_key = access_key
        self.interval = interval
        
    def get_latest_date_in_topic(self):
        consumer = KafkaConsumer(
            self.kafka_topic, 
            value_deserializer = lambda x: json.loads(x) if x != "" else 0, 
            bootstrap_servers = self.kafka_server, 
            auto_offset_reset = self.auto_offset_reset_value,
            consumer_timeout_ms = self.consumer_timeout_ms_value)
    
        messages = []
        
        for item in consumer:
            messages.append(item.value)
        
        latest_date = pd.to_datetime(messages[len(messages)-1]["data"][0]["date"]).strftime("%Y-%m-%d %H:%M:%S")
        return latest_date
    
    def get_intraday_api_response(self, date_from, offset=0):
        params = {
            'limit': self.limit,
            'offset': offset,
            'symbols': self.symbols,
            'access_key': self.access_key,
            'date_from': date_from,
            'interval': self.interval
        }
        api_result = requests.get(f"{self.base_url}/{self.path_params}", params)
        api_response = api_result.json()
        return api_response
    
    def produce(self, message):
        producer = KafkaProducer(bootstrap_servers=[self.kafka_server],
                                 value_serializer=lambda x: 
                                 dumps(x).encode(self.encoding))
        producer.send(self.kafka_topic, value=message)
    
    def main(self):
        date_from = self.get_latest_date_in_topic()
        
        try:
            while True:
                initial_api_response = self.get_intraday_api_response(date_from)
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
                        api_response = self.get_intraday_api_response(offset=offset, date_from=date_from)
                        self.produce(message = api_response)
                        print(api_response["pagination"])
                        number_of_iterations = number_of_iterations - 1
                        offset = offset - limit
                        
                    date_from = pd.to_datetime(initial_api_response["data"][0]["date"]).strftime("%Y-%m-%d %H:%M:%S")
                    time.sleep(60)
        
        except KeyboardInterrupt as kint:
            print("Stop producing")