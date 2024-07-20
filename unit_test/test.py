import os
from pyspark.sql import SparkSession
from pyspark. sql. functions import desc, asc, count
#
# for file in os.listdir("/Users/vinhnk1/Desktop/COSCO/COSTCO/data/silver"):
#     print(file)
#     print(SparkSession.builder.getOrCreate().read.parquet(f"./data/silver/{file}").printSchema())

spark = SparkSession.builder.appName('test').getOrCreate()
df = spark.read.parquet("/Users/vinhnk1/Desktop/COSCO/prepare_new_ops/data/gold/dim_customer_detail_table.parquet")
print(df.count())
print(df.groupby("CustomerId").agg(count("CustomerId").alias("cnt")).count())

