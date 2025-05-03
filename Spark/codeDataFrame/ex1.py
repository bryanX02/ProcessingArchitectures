from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import sys
import os 

spark = SparkSession.builder.appName("Distributed Grep DF").getOrCreate()

if len(sys.argv) != 4:
    print("Error: Se requieren 3 argumentos: <palabra_busqueda> <ruta_entrada> <ruta_salida>")
    sys.exit(-1)

search_word = sys.argv[1]
input_path = sys.argv[2]
output_path = sys.argv[3] # gs://BUCKET/assg2/output1rdd

df = spark.read.text(input_path)
df = df.coalesce(4)

regex_pattern = f"\\b(?i){search_word}\\b"

grep_results = df.filter(F.col("value").rlike(regex_pattern))


if not os.path.exists(output_path):
    os.makedirs(output_path)

grep_results.write.mode("overwrite").text(output_path)



# df = df.withColumn("lowerwords", F.split(F.lower(F.col("value")), " "))



# df  = df.filter(F.array_contains(F.col("lowerwords"), search_word)).select(F.col("value"))


# df.write.text("./Spark/codeDataFrame/output")