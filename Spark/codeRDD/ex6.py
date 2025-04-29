from pyspark.sql import SparkSession
import sys

# Lo mismo de los ejercicios anteriores:
spark = SparkSession.builder.appName("Taxi RDD").getOrCreate()
sc = spark.sparkContext

if len(sys.argv) != 3:
    print("Error: Se requieren 2 argumentos: <ruta_entrada_csv> <ruta_salida>")
    sys.exit(-1)
input_path = sys.argv[1]  # Ej: gs://.../yellow_tripdata_2019*.csv o local ./data/data6/yellow_tripdata_2019-01.csv
output_path = sys.argv[2] # Ej: gs://BUCKET/assg2/output6rdd

lines_rdd = sc.textFile(input_path)
lines_rdd = lines_rdd.coalesce(4)

# Filtramos la cabecera (asumiendo que empieza con "VendorID,")
data_lines_rdd = lines_rdd.filter(lambda line: not line.startswith("VendorID,"))

# Función para parsear cada línea y extraer (hora_recogida, total_amount)
def parse_taxi_line(line):
    try:
        parts = line.split(',')
        # Columna 1: tpep_pickup_datetime (ej: "2019-01-01 00:46:40")
        timestamp_str = parts[1].strip()
        total_amount_str = parts[16].strip()

        # Extraemos hora con slicing
        pickup_hour = int(timestamp_str[11:13])
        total_amount = float(total_amount_str)
        return (pickup_hour, total_amount)
    except (ValueError, IndexError, TypeError):
        return None # formato incorrecto

# Mapeamos cada línea a (hora, importe) y filtramos las inválidas
hour_amount_pairs = data_lines_rdd.map(parse_taxi_line).filter(lambda x: x is not None)

# Calculamos el importe total por hora usando reduceByKey
total_per_hour = hour_amount_pairs.reduceByKey(lambda amount1, amount2: amount1 + amount2)
# RDD resultante: (hora, importe_total_hora)

# Ordenamos por importe total (el valor). Requiere intercambiar K/V -> ordenar -> formatear
# 1. Intercambiar K/V -> (importe_total_hora, hora)
swapped_rdd = total_per_hour.map(lambda x: (x[1], x[0]))

# 2. Ordenar por la nueva clave (importe_total_hora), ascendente
sorted_swapped_rdd = swapped_rdd.sortByKey(ascending=True)

# 3. Formateamos la salida como "hora importe_total_hora"
output_lines = sorted_swapped_rdd.map(lambda x: f"{x[1]} {x[0]}") # x es (importe_total, hora)
output_lines.saveAsTextFile(output_path)