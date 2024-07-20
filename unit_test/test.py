import os
from pyspark.sql import SparkSession


for file in os.listdir("/Users/vinhnk1/Desktop/COSCO/COSTCO/data/silver"):
    print(file)
    print(SparkSession.builder.getOrCreate().read.parquet(f"./data/silver/{file}").printSchema())