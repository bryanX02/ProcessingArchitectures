from pyspark.sql import SparkSession
import sys

# Lo mismo de los ejercicios anteriores:
spark = SparkSession.builder.appName("Movie Rating Data RDD").getOrCreate()
sc = spark.sparkContext

if len(sys.argv) != 3:
    print("Error: Se requieren 2 argumentos: <ruta_entrada_csv> <ruta_salida>")
    sys.exit(-1)
input_path = sys.argv[1]  # Ej: gs://cloudandbigdata/ml-latest/ratings.csv o local ./data/ml-latest-small/ratings.csv
output_path = sys.argv[2] # Ej: gs://BUCKET/assg2/output4rdd

lines_rdd = sc.textFile(input_path)
lines_rdd = lines_rdd.coalesce(4)

# Filtramos la cabecera (asumiendo que es "userId,movieId,rating,timestamp")
data_lines_rdd = lines_rdd.filter(lambda line: not line.startswith("userId,"))

# Función para parsear cada línea y extraer (movieId, rating)
def parse_rating_line(line):
    try:
        parts = line.split(',')
        movie_id = int(parts[1]) # Columna 1: movieId
        rating = float(parts[2]) # Columna 2: rating
        return (movie_id, rating)
    except (ValueError, IndexError, TypeError):
        return None # formato incorrecto

# Mapeamos cada línea a (movieId, rating) y filtramos las líneas inválidas
movie_rating_pairs = data_lines_rdd.map(parse_rating_line).filter(lambda x: x is not None)

# Calculamos (suma_ratings, num_registros) para cada movieId usando combineByKey
sum_count_per_movie = movie_rating_pairs.combineByKey(
    lambda rating: (rating, 1),                    # Inicializador: (rating, 1)
    lambda acc, rating: (acc[0] + rating, acc[1] + 1), # Combinador intra-partición
    lambda acc1, acc2: (acc1[0] + acc2[0], acc1[1] + acc2[1]) # Combinador inter-partición
)

# Calculamos el rating promedio: suma_ratings / num_registros
avg_rating_per_movie = sum_count_per_movie.mapValues(
    lambda sum_count: sum_count[0] / sum_count[1] if sum_count[1] != 0 else 0 # Evita división por cero
)

# Función para asignar el rango según el rating promedio
def get_rating_range(avg_rating):
    if 0 <= avg_rating <= 1: return "Range 1" #[0, 1]
    elif avg_rating <= 2: return "Range 2"    #(1, 2]
    elif avg_rating <= 3: return "Range 3"    #(2, 3]
    elif avg_rating <= 4: return "Range 4"    #(3, 4]
    elif avg_rating <= 5: return "Range 5"    #(4, 5]
    else: return "Invalid Range" # Para valores fuera de [0, 5]

# Asignamos el rango a cada película usando la función anterior
movie_range_rdd = avg_rating_per_movie.mapValues(get_rating_range)

# Y finalmente formateamos la salida como "movieId Rango"
output_lines = movie_range_rdd.map(lambda x: f"{x[0]} {x[1]}") # x es (movieId, Rango)
output_lines.saveAsTextFile(output_path)