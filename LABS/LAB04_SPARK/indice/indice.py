from pyspark import SparkContext
import os

def limpiar_palabras(texto):
    return texto.lower().replace('.', '').replace(',', '').split()

def extraer_nombre_archivo(ruta_completa):
    return os.path.basename(ruta_completa)


sc = SparkContext("local[*]", "IndiceInvertido")

archivos = sc.wholeTextFiles("documentos/")

# (palabra, nombre_archivo_sin_ruta)
palabras_documentos = archivos.flatMap(
    lambda x: [(palabra, extraer_nombre_archivo(x[0])) for palabra in limpiar_palabras(x[1])]
)

# Agrupar los archivos por palabra y eliminar duplicados
indice_invertido = palabras_documentos.groupByKey() \
    .mapValues(lambda docs: sorted(list(set(docs))))  # ordenamos por claridad

# Recolectar resultados
resultados = indice_invertido.collect()


# Escribir resultados a un archivo
with open("indice_invertido.txt", "w", encoding="utf-8") as f:
    for palabra, archivos in resultados:
        f.write(f"{palabra}:\t {', '.join(archivos)}\n")

print("Índice invertido guardado en 'indice_invertido.txt'")

sc.stop()
