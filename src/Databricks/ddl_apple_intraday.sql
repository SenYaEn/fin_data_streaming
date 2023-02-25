-- Databricks notebook source
DROP TABLE IF EXISTS bronze.apple_intraday;

CREATE TABLE bronze.apple_intraday
(
    open FLOAT
  , high FLOAT
  , low FLOAT
  , `last` FLOAT
  , close FLOAT
  , volume FLOAT
  , `timestamp` TIMESTAMP
  , symbol STRING
  , `exchange` STRING
  , date_key INT
)
PARTITIONED BY (date_key)
;
