from pyspark.sql import SparkSession
import sys
import re

# Iniciamos la sesión de Spark y obtenemos el SparkContext
spark = SparkSession.builder.appName("Count URL Access Frequency RDD").getOrCreate()
sc = spark.sparkContext

# Verificamos y obtenemos la entrada y la salida
if len(sys.argv) != 3:
    print("Error: Se requieren 2 argumentos: <ruta_entrada> <ruta_salida>")
    sys.exit(-1)

input_path = sys.argv[1]  # gs://cloudandbigdata/access_log
output_path = sys.argv[2] # gs://BUCKET/assg2/output2rdd

# Leemos los archivos de log como un RDD de líneas
lines_rdd = sc.textFile(input_path)

# Reducimos el número de particiones si es necesario
lines_rdd = lines_rdd.coalesce(4)

# Expresión regular para extraer la URL base del log CLF
log_pattern = re.compile(r'\"[A-Z]+ ([^?\s]*)(?:\?[^ ]*)? [A-Z]+/[\d\.]+\"')

# Función para extraer la URL de una línea de log
def extract_url(line):
    match = log_pattern.search(line)
    return [match.group(1)] if match else []

# Extraemos las URLs usando flatMap (descarta líneas sin match)
urls_rdd = lines_rdd.flatMap(extract_url)

# Contamos la frecuencia de cada URL: (URL, 1) -> reduce sumando contadores
url_counts_rdd = urls_rdd.map(lambda url: (url, 1)) \
                       .reduceByKey(lambda count1, count2: count1 + count2)

# Obtenemos las 100 URLs más frecuentes, en el lambda x = (url, count)
top_100_list = url_counts_rdd.takeOrdered(100, key=lambda x: -x[1]) 

# Formateamos la lista resultante a "url count"
formatted_top_100 = [f"{url} {count}" for url, count in top_100_list]

sc.parallelize(formatted_top_100, 1).saveAsTextFile(output_path)