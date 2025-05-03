from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import sys
import os 

spark = SparkSession.builder.appName("Movie Rating Data DF").getOrCreate()


if len(sys.argv) != 3:
    print("Error: Se requieren 2 argumentos: <ruta_entrada_csv> <ruta_salida>")
    sys.exit(-1)
input_path = sys.argv[1]  # Ej: gs://cloudandbigdata/Meteorite_Landings.csv o local ./data/Meteorite_Landings/Meteorite_Landings.csv
output_path = sys.argv[2] # Ej: gs://BUCKET/assg2/output5rdd

df = spark.read.csv(input_path, header=True, inferSchema=True, sep=";")
df = df.coalesce(4)

df = df.filter(F.col("mass (g)").isNotNull())

df_group = df.groupBy("recclass").agg(F.avg(F.col("mass (g)")).alias("avg_mass")).orderBy("recclass")



if not os.path.exists(output_path):
    os.makedirs(output_path)

df_group.write.mode("overwrite").option("sep", " ").csv(output_path, header=False)



