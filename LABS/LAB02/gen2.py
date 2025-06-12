import os
import random

# Configuración
target_size = 1 * 1024 * 1024 * 1024  # 1GB por archivo
num_files = 20
words_to_select = 15000
word_list_file = "words.txt"  # Asegúrate de que este archivo contenga 100,000 palabras

# Cargar lista de palabras
if not os.path.exists(word_list_file):
    raise FileNotFoundError("words.txt no encontrado. Asegúrate de que exista.")
with open(word_list_file, 'r', encoding='utf-8') as f:
    full_word_list = [line.strip() for line in f if line.strip()]

if len(full_word_list) < words_to_select:
    raise ValueError(f"El archivo words.txt debe tener al menos {words_to_select} palabras únicas.")

# Crear directorio si no existe
os.makedirs("Data", exist_ok=True)

# Generar archivos
for i in range(num_files):
    output_file = f"Data/1GB_{i+1}.txt"
    print(f"Creando archivo: {output_file}")

    # Seleccionar 15,000 palabras aleatorias para este archivo
    selected_words = random.sample(full_word_list, words_to_select)

    with open(output_file, 'w', encoding='utf-8') as f:
        current_size = 0
        chunk_size = 10 * 1024 * 1024  # Procesar en bloques de 10MB
        words_per_chunk = chunk_size // 10  # Asumiendo ~10 bytes por palabra

        while current_size < target_size:
            chunk = ' '.join(random.choices(selected_words, k=words_per_chunk))
            f.write(chunk + '\n')
            f.flush()
            current_size = os.path.getsize(output_file)

            # Opcional: Progreso cada 100MB
            if current_size % (100 * 1024 * 1024) < 1024:
                print(f"  Progreso: {current_size / (1024**3):.2f}GB")

    print(f"✓ Finalizado {output_file} - Tamaño: {current_size / (1024**3):.2f}GB\n")
