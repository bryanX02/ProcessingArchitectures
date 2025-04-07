from pyspark import SparkConf, SparkContext
import sys

if "spark" in locals():
    spark.stop()  # Detiene la sesión actual
    del spark

conf = SparkConf().setAppName('CountURL').setMaster("local[*]").set("spark.hadoop.fs.defaultFS", "file:///")
sc = SparkContext.getOrCreate(conf)

input_file = sys.argv[1]


lines = sc.textFile(input_file).coalesce(8)
# Lista para almacenar las URLs y sus frecuencias
top_urls = []

# Leer entrada desde stdin
for line in lines:
    key, value = sc.parallelize(line.split())\
        .map(lambda x: (x, 1)) .reduce(lambda x, y: x + y)

    # Insertar ordenadamente en la lista
    top_urls.append((key, value))
    top_urls.sort(key=lambda x: x[1], reverse=True)

    # Mantener solo las 20 entradas más grandes
    if len(top_urls) > 100:
        top_urls.pop()

# Imprimir las 20 URLs más frecuentes
for key, value in top_urls:
    print(f"{key}\t{value}")
