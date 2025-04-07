from pyspark import SparkConf, SparkContext
import sys

if "spark" in locals():
    spark.stop()  # Detiene la sesión actual
    del spark

conf = SparkConf().setAppName('DistributedGrep').setMaster("local[*]").set("spark.hadoop.fs.defaultFS", "file:///")
sc = SparkContext.getOrCreate(conf)

input_file = sys.argv[1]
search_word = sys.argv[2].lower()


lines = sc.textFile(input_file).coalesce(8)
matching_lines = lines.filter(lambda line: search_word in line.lower())

matching_lines.saveAsTextFile('output8')
