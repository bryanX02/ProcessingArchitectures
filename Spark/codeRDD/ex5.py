from pyspark.sql import SparkSession
import sys

# Lo mismo de los ejercicios anteriores:
spark = SparkSession.builder.appName("Meteorite Landing RDD").getOrCreate() # Nombre de App corregido
sc = spark.sparkContext

if len(sys.argv) != 3:
    print("Error: Se requieren 2 argumentos: <ruta_entrada_csv> <ruta_salida>")
    sys.exit(-1)
input_path = sys.argv[1]  # Ej: gs://cloudandbigdata/Meteorite_Landings.csv o local ./data/Meteorite_Landings/Meteorite_Landings.csv
output_path = sys.argv[2] # Ej: gs://BUCKET/assg2/output5rdd

lines_rdd = sc.textFile(input_path)
lines_rdd = lines_rdd.coalesce(4)

# Filtramos la cabecera (asumiendo que empieza con "name;")
# Ojo que el separador en este CSV es ';'
data_lines_rdd = lines_rdd.filter(lambda line: not line.startswith("name;"))

# Función para parsear cada línea y extraer (recclass, mass)
def parse_meteorite_line(line):
    try:
        parts = line.split(';') # Separador es punto y coma ';'
        recclass = parts[3].strip() # Columna 3: recclass (tipo)
        mass_str = parts[4].strip() # Columna 4: mass (g) (masa)

        # Filtramos registros sin masa (equivalente a isNotNull() en DF)
        if not mass_str:
            return None # Ignoramos este registro

        mass = float(mass_str) # Convertimos masa a número
        return (recclass, mass)
    except (ValueError, IndexError, TypeError):
        return None # formato incorrecto

# Mapeamos cada línea a (recclass, mass) y filtramos las inválidas/sin masa
type_mass_pairs = data_lines_rdd.map(parse_meteorite_line).filter(lambda x: x is not None)

# Calculamos (suma_masa, num_registros) para cada recclass usando combineByKey
sum_count_per_type = type_mass_pairs.combineByKey(
    lambda mass: (mass, 1),                      # Inicializador: (masa, 1)
    lambda acc, mass: (acc[0] + mass, acc[1] + 1),   # Combinador intra-partición: acumula suma y cuenta
    lambda acc1, acc2: (acc1[0] + acc2[0], acc1[1] + acc2[1]) # Combinador inter-partición: combina acumuladores
)

# Calculamos la masa promedio: suma_masa / num_registros
avg_mass_per_type = sum_count_per_type.mapValues(
    lambda sum_count: sum_count[0] / sum_count[1] if sum_count[1] != 0 else 0 # Evita división por cero
)

# Ordenamos los resultados por recclass (la clave del RDD)
sorted_results = avg_mass_per_type.sortByKey(ascending=True)

# Y finalmente formateamos la salida como "recclass masa_promedio"
output_lines = sorted_results.map(lambda x: f"{x[0]} {x[1]}") # x es (recclass, masa_promedio)
output_lines.saveAsTextFile(output_path)