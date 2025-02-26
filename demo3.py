from pyspark.sql.functions import *

from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()


df = spark.read.csv("original.csv", header=True,inferSchema=True)

df=df.where(df.id < 10)


df.write.csv(path="folder/abc.csv",mode="overwrite")