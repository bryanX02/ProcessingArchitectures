from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import os 

spark = SparkSession.builder.appName("Stock Summary").getOrCreate()


df = spark.read.csv("./data/GOOG.csv", header=True, inferSchema=True)
df = df.coalesce(4)


df = df.withColumn("year", F.year(F.col("Date")) )


df_group = df.groupBy("year").agg(F.avg(F.col("Close"))).orderBy(F.col("year"))


output_dir = "./Spark/codeDataFrame/output3/"
if not os.path.exists(output_dir):
    os.makedirs(output_dir)

df_group.write.mode("overwrite").option("sep", " ").csv(output_dir, header=False)



