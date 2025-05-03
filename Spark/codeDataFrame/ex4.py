from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import sys
import os 

spark = SparkSession.builder.appName("Movie Rating Data DF").getOrCreate()

if len(sys.argv) != 3:
    print("Error: Se requieren 2 argumentos: <ruta_entrada_csv> <ruta_salida>")
    sys.exit(-1)
input_path = sys.argv[1]  # Ej: gs://cloudandbigdata/ml-latest/ratings.csv o local ./data/ml-latest-small/ratings.csv
output_path = sys.argv[2] # Ej: gs://BUCKET/assg2/output4rdd

df = spark.read.csv(input_path, header=True, inferSchema=True)
df = df.coalesce(4)

df_group = df.groupBy("movieId").agg(F.avg(F.col("rating")).alias("avg_rating"))

rated_movies = df_group.withColumn(
    "rango",
    F.when((F.col("avg_rating") >= 0) & (F.col("avg_rating") <= 1), "Range 1") \
    .when((F.col("avg_rating") > 1) & (F.col("avg_rating") <= 2), "Range 2") \
    .when((F.col("avg_rating") > 2) & (F.col("avg_rating") <= 3), "Range 3") \
    .when((F.col("avg_rating") > 3) & (F.col("avg_rating") <= 4), "Range 4") \
    .when((F.col("avg_rating") > 4) & (F.col("avg_rating") <= 5), "Range 5")
).select(F.col("movieId"), F.col("rango"))
        


if not os.path.exists(output_path):
    os.makedirs(output_path)

rated_movies.write.mode("overwrite").option("sep", " ").csv(output_path, header=False)



