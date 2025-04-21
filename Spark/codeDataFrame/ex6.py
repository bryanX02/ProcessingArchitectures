from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import os 

spark = SparkSession.builder.appName("Taxi").getOrCreate()


df = spark.read.csv("./data/data6/yellow_tripdata_2019-01.csv", header=True, inferSchema=True, sep=",")
df = df.coalesce(4)


df_with_hour = df.withColumn("pickup_hour", F.hour(F.col("tpep_pickup_datetime")))

total_por_hora = df_with_hour.groupBy("pickup_hour") \
    .agg(F.sum("total_amount").alias("total_ganado"))

# 5. Ordenar por la hora
total_por_hora_ordenado = total_por_hora.orderBy("total_ganado")

output_dir = "./Spark/codeDataFrame/output6/"
if not os.path.exists(output_dir):
    os.makedirs(output_dir)

total_por_hora_ordenado .write.mode("overwrite").option("sep", " ").csv(output_dir, header=False)



