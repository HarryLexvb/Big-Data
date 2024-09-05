import os
import time
import random
from concurrent.futures import ProcessPoolExecutor
import nltk
from nltk.corpus import words

nltk.download('words', quiet=True)

word_list = words.words()

word_list = [word for word in word_list if word.isalpha()]

SAVE_PATH = r"C:/Users/harry/Documents/files1"

def generate_file(file_name, size_gb=1):
    target_size = size_gb * 1024 * 1024 * 1024  # Convertir GB a bytes
    chunk_size = 1024 * 1024  # 1MB por chunk

    full_path = os.path.join(SAVE_PATH, file_name)

    with open(full_path, 'w') as f:
        written_size = 0
        while written_size < target_size:
            chunk = ' '.join(random.choices(word_list, k=chunk_size // 10)) + ' '
            if written_size + len(chunk) > target_size:
                chunk = chunk[:target_size - written_size]
            f.write(chunk)
            written_size += len(chunk)

def main():
   
    os.makedirs(SAVE_PATH, exist_ok=True)

    start_time = time.time()
    num_files = 20
    file_names = [f"large_file_{i}.txt" for i in range(num_files)]

    with ProcessPoolExecutor() as executor:
        executor.map(generate_file, file_names)

    end_time = time.time()
    total_time = end_time - start_time
    print(f"Tiempo total de ejecución: {total_time:.2f} segundos")
    print(f"Los archivos se han guardado en: {SAVE_PATH}")

if __name__ == "__main__":
    main()