from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import os 

spark = SparkSession.builder.appName("Movie Rating Data").getOrCreate()


df = spark.read.csv("./data/ml-latest-small/ratings.csv", header=True, inferSchema=True)
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
        

output_dir = "./Spark/codeDataFrame/output4/"
if not os.path.exists(output_dir):
    os.makedirs(output_dir)

rated_movies.write.mode("overwrite").option("sep", " ").csv(output_dir, header=False)



