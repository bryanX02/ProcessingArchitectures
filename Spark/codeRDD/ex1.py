from pyspark.sql import SparkSession
import sys
import re

# Iniciamos la sesión de Spark y obtenemos el SparkContext
spark = SparkSession.builder.appName("DistributedGrep").getOrCreate()
sc = spark.sparkContext

# Aunque no este en el enunciado determinamos que seria adecuado recibir 3 argumentos
if len(sys.argv) != 4:
    print("Error: Se requieren 3 argumentos: <palabra_busqueda> <ruta_entrada> <ruta_salida>")
    sys.exit(-1)

search_word = sys.argv[1]
input_path = sys.argv[2]
output_path = sys.argv[3] # gs://BUCKET/assg2/output1rdd

# Leemos los archivos de texto de entrada como un RDD de líneas
lines_rdd = sc.textFile(input_path)

# Reducimos el número de particiones (N = num threads)
lines_rdd = lines_rdd.coalesce(4)

# Compilamos la expresión regular para buscar la palabra completa, ignorando mayúsculas/minúsculas
# \\b -> límite de palabra, re.IGNORECASE -> insensible a mayúsculas
pattern = re.compile(f"\\b{search_word}\\b", re.IGNORECASE)

# Filtramos el RDD: mantenemos solo las líneas que contienen la palabra buscada
grep_results_rdd = lines_rdd.filter(lambda line: pattern.search(line))

# Y guardamos
grep_results_rdd.saveAsTextFile(output_path)