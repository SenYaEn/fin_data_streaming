# fin_data_streaming

This repository contains a sample solution which pulls real-time stock trading data provided by [Marketstack](https://marketstack.com/) REST API, processes it using a set of tools described below and makes it available for end-user consumption via a BI tool like Power BI.

## Infrastructure
The solution leverages the following resources:
1. REST API
2. Apache Kafka
3. AWS S3 Bucket
4. Azure Databricks
5. Databricks Auto Loader
6. Azure Key Vault
7. Data Lake
8. Power BI

Below is a High-Level solution architechture diagram.

![image](https://user-images.githubusercontent.com/58121577/221368586-9ed72d17-1a8d-4506-9c80-bfd6a8bedb52.png)

### REST API
REST API used in this solution is the Marketstack REST API interface delivering worldwide stock market data in JSON format. The interface connecting to Marketstack end-points is incorporated in the ```ProducerMarketstack``` Python class. Below are some sample methods which are part of the REST API interface within the producer:

```python
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
```

and 

```python
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
```

### Apache Kafka
Apache Kafka is used in this solution as a Proof of Concept (POC) and would add more benefit in a production system with multiple producers and consumers of the same data streams.
In the current solution there's only one producer ```ProducerMarketstack``` and one consumer ```ConsumerMarketstack``` defined.

### AWS S3 Bucket
AWS S3 Bucket was used in this solution to test the ease of integration between the Azure Databricks Auto Loader and data sources outside of Azure. The solution has proven that the integration is very smooth.

### Azure Databricks
Azure Databricks is the main processing engine in the solution parsing the JSON files and storing them in a Delta table on Data Lake. Databricks cluster is also used by Power BI to query and manipulate the data.

### Databricks Auto Loader
Auto Loader incrementally and efficiently processes new data files as they arrive in S3 Bucket and is implemented in ```stream_apple_intraday``` Databricks notebook. Within the solution it can run either in a triggered or continuous modes.
Continuous and triggered modes are controled by the `trigger_once` parameter:

```python
if trigger_once == True:
    (streamDf.writeStream 
     .trigger(once=True) # this option allows to run the data load on schedule instead of continuous mode
     .format("delta")
     .outputMode("append")
     .option("checkpointLocation", "/mnt/lake/bronze/apple_intraday/_checkpoints/")
     .toTable("bronze.apple_intraday"))
else:
    (streamDf.writeStream
     .format("delta")
     .outputMode("append")
     .option("checkpointLocation", "/mnt/lake/bronze/apple_intraday/_checkpoints/")
     .toTable("bronze.apple_intraday"))
```

### Azure Key Vault
Key Vault is used within this solution to store the S3 Bucket, Data Lake, and Databricks credentials. The Vault is mounted on Databricks using [Secret Scopes](https://docs.microsoft.com/en-us/azure/databricks/security/secrets/secret-scopes).

### Data Lake
Data Lake is used to store the ready-for-consumption Delta lake table data.

### Power BI
Power BI can be one of the BI tools consuming the streaming data produced by this solution. Below is a sample dashboard showing trading data for Apple Inc. trading data:

![image](https://user-images.githubusercontent.com/58121577/221370475-37a88c4c-ead6-4c6b-bbac-3e813054863e.png)






