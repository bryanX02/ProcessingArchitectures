from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import os 

spark = SparkSession.builder.appName("Movie Rating Data").getOrCreate()


df = spark.read.csv("./data/Meteorite_Landings/Meteorite_Landings.csv", header=True, inferSchema=True, sep=";")
df = df.coalesce(4)

df = df.filter(F.col("mass (g)").isNotNull())

df_group = df.groupBy("recclass").agg(F.avg(F.col("mass (g)")).alias("avg_mass")).orderBy("recclass")


output_dir = "./Spark/codeDataFrame/output5/"
if not os.path.exists(output_dir):
    os.makedirs(output_dir)

df_group.write.mode("overwrite").option("sep", " ").csv(output_dir, header=False)



