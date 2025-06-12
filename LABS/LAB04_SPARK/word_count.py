from pyspark.sql import SparkSession

# Crear sesión de Spark
spark = SparkSession.builder \
    .appName("WordCount") \
    .getOrCreate()

# Crear contexto de Spark
sc = spark.sparkContext

# Crear un RDD con un texto de ejemplo
texto = [
    "Hola mundo esto es Spark",
    "Spark es una herramienta poderosa para Big Data",
    "Contar palabras con Spark es sencillo"
]

# Paralelizar el texto (crear el RDD)
rdd = sc.parallelize(texto)

# Transformaciones: dividir líneas en palabras, mapear y contar
word_counts = rdd.flatMap(lambda linea: linea.split()) \
                 .map(lambda palabra: (palabra.lower(), 1)) \
                 .reduceByKey(lambda a, b: a + b)

# Mostrar los resultados
for palabra, conteo in word_counts.collect():
    print(f"{palabra}: {conteo}")
