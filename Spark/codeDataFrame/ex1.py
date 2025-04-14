from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import sys

spark = SparkSession.builder.appName("Distributed Grep").getOrCreate()

if len(sys.argv) > 1:
    search_word = sys.argv[1].lower()
else:
    raise KeyError("No se ha especificado la palabra a buscar (p.e 'zoroaster')")

df = spark.read.text("./data/input.txt")
df = df.coalesce(4)

regex_pattern = f"\\b(?i){search_word}\\b"

grep_results = df.filter(F.col("value").rlike(regex_pattern))

grep_results.write.mode("overwrite").text("./Spark/codeDataFrame/output")


# df = df.withColumn("lowerwords", F.split(F.lower(F.col("value")), " "))



# df  = df.filter(F.array_contains(F.col("lowerwords"), search_word)).select(F.col("value"))


# df.write.text("./Spark/codeDataFrame/output")