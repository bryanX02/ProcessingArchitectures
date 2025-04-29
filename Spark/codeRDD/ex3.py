from pyspark.sql import SparkSession
import sys

# Lo mismo de los ejecicios anteriores:
spark = SparkSession.builder.appName("Stock Summary RDD").getOrCreate()
sc = spark.sparkContext

if len(sys.argv) != 3:
    print("Error: Se requieren 2 argumentos: <ruta_entrada_csv> <ruta_salida>")
    sys.exit(-1)
input_path = sys.argv[1]  # gs://cloudandbigdata/Nasdaq100/*.csv o local ./data/GOOG.csv
output_path = sys.argv[2] # gs://BUCKET/assg2/output3rdd

lines_rdd = sc.textFile(input_path)
lines_rdd = lines_rdd.coalesce(4)

# Filtramos la cabecera
data_lines_rdd = lines_rdd.filter(lambda line: not line.startswith("Date,"))

# Función para parsear cada línea y extraer (año, precio_cierre)
def parse_stock_line(line):
    try:
        parts = line.split(',')
        year = int(parts[0][:4])      # Columna 0: Fecha (YYYY-MM-DD), extraemos YYYY
        close_price = float(parts[4]) # Columna 4: Precio de cierre (Close)
        return (year, close_price)
    except (ValueError, IndexError):
        return None # formato incorrecto

# Mapeamos cada línea a (año, precio_cierre) y filtramos las líneas inválidas
year_price_pairs = data_lines_rdd.map(parse_stock_line).filter(lambda x: x is not None)

# Calculamos (suma_precios, num_registros) para cada año usando combineByKey
sum_count_per_year = year_price_pairs.combineByKey(
    lambda price: (price, 1),                  # Inicializador: (precio, 1) por cada clave nueva (año)
    lambda acc, price: (acc[0] + price, acc[1] + 1), # Combinador intra-partición: acumula suma y cuenta
    lambda acc1, acc2: (acc1[0] + acc2[0], acc1[1] + acc2[1]) # Combinador inter-partición: combina acumuladores
)

# Calculamos el promedio: suma_precios / num_registros
avg_price_per_year = sum_count_per_year.mapValues(
    lambda sum_count: sum_count[0] / sum_count[1] if sum_count[1] != 0 else 0 # Evita división por cero
)

# Ordenamos los resultados por año (la clave del RDD)
sorted_results = avg_price_per_year.sortByKey(ascending=True)

# Y finalmente formateamos la salida como "año promedio"
output_lines = sorted_results.map(lambda x: f"{x[0]} {x[1]}") # x es (año, promedio)
output_lines.saveAsTextFile(output_path)