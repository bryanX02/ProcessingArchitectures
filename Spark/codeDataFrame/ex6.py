from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import sys
import os 

spark = SparkSession.builder.appName("Taxi DF").getOrCreate()

if len(sys.argv) != 3:
    print("Error: Se requieren 2 argumentos: <ruta_entrada_csv> <ruta_salida>")
    sys.exit(-1)
input_path = sys.argv[1]  # Ej: gs://.../yellow_tripdata_2019*.csv o local ./data/data6/yellow_tripdata_2019-01.csv
output_path = sys.argv[2] # Ej: gs://BUCKET/assg2/output6rdd

df = spark.read.csv(input_path, header=True, inferSchema=True, sep=",")
df = df.coalesce(4)


df_with_hour = df.withColumn("pickup_hour", F.hour(F.col("tpep_pickup_datetime")))

total_por_hora = df_with_hour.groupBy("pickup_hour") \
    .agg(F.sum("total_amount").alias("total_ganado"))

# 5. Ordenar por la hora
total_por_hora_ordenado = total_por_hora.orderBy("total_ganado")


if not os.path.exists(output_path):
    os.makedirs(output_path)

total_por_hora_ordenado .write.mode("overwrite").option("sep", " ").csv(output_path, header=False)



