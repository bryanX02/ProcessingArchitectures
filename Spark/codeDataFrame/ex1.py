from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import sys

spark = SparkSession.builder.appName("Distributed Grep").getOrCreate()
search_word = sys.argv[1].lower()
df = spark.read.text("./data/input.txt")
df = df.coalesce(4)

df = df.withColumn("lowerwords", F.split(F.lower(F.col("value")), " "))



df  = df.filter(F.array_contains(F.col("lowerwords"), search_word)).select(F.col("value"))


df.write.text("./Spark/codeDataFrame/output")