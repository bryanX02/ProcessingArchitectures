from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import sys
import os 

spark = SparkSession.builder.appName("Count URL Access Frequency").getOrCreate()


df = spark.read.text("./data/access_log-small/access_log-small")
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


output_dir = "./Spark/codeDataFrame/output2/"
if not os.path.exists(output_dir):
    os.makedirs(output_dir)

url_counts.write.mode("overwrite").option("sep", " ").csv(output_dir, header=False)
