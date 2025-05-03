from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import sys
import os 

spark = SparkSession.builder.appName("Count URL Access Frequency DF").getOrCreate()

if len(sys.argv) != 3:
    print("Error: Se requieren 2 argumentos: <ruta_entrada> <ruta_salida>")
    sys.exit(-1)

input_path = sys.argv[1]  # gs://cloudandbigdata/access_log
output_path = sys.argv[2] # gs://BUCKET/assg2/output2rdd

df = spark.read.text(input_path)
df = df.coalesce(4)



url_df = df.withColumn(
    "url",
    F.regexp_extract(
        F.col("value"),
        r'\"[A-Z]+ ([^?\s]*)(?:\?[^ ]*)? [A-Z]+/[\d\.]+\"',
        1
    )
)


url_counts = url_df.groupBy("url").count() \
    .orderBy(F.desc("count")) \
    .limit(100)



if not os.path.exists(output_path):
    os.makedirs(output_path)

url_counts.write.mode("overwrite").option("sep", " ").csv(output_path, header=False)
