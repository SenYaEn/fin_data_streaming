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
    """Defines a Producer object for Marketstack API service as a source
    and Kafka topic as a sink.
    
    Class parameters:
    :base_url: API related parameter - base URL
    :encoding: Format used when encoding a Kafka message
    :path_params: API related parameter containing the endpoint
    :limit: API related parameter containing the limit of records per page
    
    Instance parameters:
    :kafka_topic: Kafka topic to which messages are to be posted
    :kafka_server: Kafka bootstrap server
    :symbols: API related parameter containing the stock code for which data is to be pulled
    :access_key: API related parameter containing API access key
    :interval: API related parameter containing the time grain level at which the stock prices should be provided
    :auto_offset_reset_value: Kafka topic offset reset value
    :consumer_timeout_ms_value: Time period in microsecond after which Kafka consumer should shut down
    """
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
        """Returns last date value (Marketstack API payload specific) based 
        on the last commited message in the topic
        """
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
        """Returns API response based on the provided parameters"""
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
        """Produces messages to the specified Kafka topic"""
        producer = KafkaProducer(bootstrap_servers=[self.kafka_server],
                                 value_serializer=lambda x: 
                                 dumps(x).encode(self.encoding))
        producer.send(self.kafka_topic, value=message)
    
    def main(self):
        """Runs the full cycle data pull starting with getting the most recent date value 
        (Marketstack API payload specific) from Kafka topic, making the necessary number of 
        API calls to get all the messages not consumed from the last date in the topic 
        (output of get_latest_date_in_topic()) and ending with posting the message(s) to 
        Kafka topic
        """
        date_from = self.get_latest_date_in_topic()
        
        try:
            while True:
                # Initial API call is made to determine how many pages are in the payload
                initial_api_response = self.get_intraday_api_response(date_from)
                limit = initial_api_response["pagination"]["limit"]
                total = initial_api_response["pagination"]["total"]
                total_to_limit_ratio = total/limit
                number_of_iterations = math.ceil(total_to_limit_ratio)
                last_date = pd.to_datetime(initial_api_response["data"][0]["date"]).strftime("%Y-%m-%d %H:%M:%S")
                
                # Checks if the most recent date in the API response is the same as the last date in Kafka topic
                if last_date == date_from:
                    print("last_date: " + str(last_date) + " ; " + "date_from: " + str(date_from))
                    print("No fresh data to load. Timestamp: " + str(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
                    time.sleep(120)
                
                # Posts to Kafka topic only if there's new records to load from API payload
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