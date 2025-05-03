from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import sys
import os 

spark = SparkSession.builder.appName("Stock Summary DF").getOrCreate()

if len(sys.argv) != 3:
    print("Error: Se requieren 2 argumentos: <ruta_entrada_csv> <ruta_salida>")
    sys.exit(-1)
input_path = sys.argv[1]  # gs://cloudandbigdata/Nasdaq100/*.csv o local ./data/GOOG.csv
output_path = sys.argv[2] # gs://BUCKET/assg2/output3rdd


df = spark.read.csv(input_path, header=True, inferSchema=True)
df = df.coalesce(4)


df = df.withColumn("year", F.year(F.col("Date")) )


df_group = df.groupBy("year").agg(F.avg(F.col("Close"))).orderBy(F.col("year"))



if not os.path.exists(output_path):
    os.makedirs(output_path)

df_group.write.mode("overwrite").option("sep", " ").csv(output_path, header=False)



