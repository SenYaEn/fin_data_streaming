# Databricks notebook source
schema_location = dbutils.widgets.get('schema_location') # s3://marketstack-intraday/schemas
files_path = dbutils.widgets.get('files_path') # s3://marketstack-intraday/
trigger_once = dbutils.widgets.get('trigger_once') # True

ACCESS_KEY = dbutils.secrets.get(scope = "DevOpsSecret", key = "s3Key")
SECRET_KEY = dbutils.secrets.get(scope = "DevOpsSecret", key = "s3Secret")

sc._jsc.hadoopConfiguration().set("fs.s3n.awsAccessKeyId", ACCESS_KEY)
sc._jsc.hadoopConfiguration().set("fs.s3n.awsSecretAccessKey", SECRET_KEY)

# COMMAND ----------

from datetime import datetime
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

schema = StructType([ 
    StructField("open",FloatType(),True), 
    StructField("high",FloatType(),True), 
    StructField("low",FloatType(),True), 
    StructField("last",FloatType(),True), 
    StructField("close",FloatType(),True), 
    StructField("volume",FloatType(),True), 
    StructField("date",TimestampType(),True), 
    StructField("symbol",StringType(),True), 
    StructField("exchange", StringType(), True)
  ])

# COMMAND ----------

streamDf = (spark.readStream.format("cloudFiles") # cloudFiles as a format automatically assumes Auto Loader is used
                            .option("cloudFiles.format", "json")
                            .option("cloudFiles.schemaLocation", schema_location) # location is needed to ensure schema evolution
                            .option("cloudFiles.schemaHints", "data ARRAY<STRING>") # Auto Loader extracts the schema but sometimes it needs hints if some columns aren't extracted correctly 
                            .load(files_path)
                            .withColumn("exploaded_data", explode("data")) # after the data started loading a streaming df is created and hence can be manipulated
                            .withColumn("json_data",from_json("exploaded_data", schema))
                            .withColumn("open", col("json_data.open"))
                            .withColumn("high", col("json_data.high"))
                            .withColumn("low", col("json_data.low"))
                            .withColumn("last", col("json_data.last"))
                            .withColumn("close", col("json_data.close"))
                            .withColumn("volume", col("json_data.volume"))
                            .withColumn("timestamp", col("json_data.date"))
                            .withColumn("symbol", col("json_data.symbol"))
                            .withColumn("exchange", col("json_data.exchange"))
                            .withColumn("date_key", date_format(col("json_data.date"), "yyyyMMdd").cast(IntegerType()))
                            .select(["open", "high", "low", "last", "close", "volume", "timestamp", "symbol", "exchange", "date_key"]) 
                            .withWatermark("timestamp", "1800 seconds") # withWatermark sets the time limit at which duplicates whould be checked and removed by dropDuplicates
                            .dropDuplicates(["timestamp"]))

# COMMAND ----------

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
