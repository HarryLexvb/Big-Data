import os
import re
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor
import time


def process_file_chunk(file_path, chunk_size=1000000):  # 1MB chunks
    file_name = os.path.basename(file_path)
    word_locations = defaultdict(set)

    with open(file_path, 'r', encoding='utf-8', errors='ignore') as file:
        chunk = file.read(chunk_size)
        while chunk:
            words = re.findall(r'\b\w+\b', chunk.lower())
            for word in words:
                word_locations[word].add(file_name)
            chunk = file.read(chunk_size)

    return word_locations


def merge_results(results):
    merged = defaultdict(set)
    for result in results:
        for word, locations in result.items():
            merged[word].update(locations)
    return merged


def write_results(merged_results, output_file, batch_size=10000):
    with open(output_file, 'w', encoding='utf-8') as out_file:
        batch = []
        for word, locations in sorted(merged_results.items()):
            locations_str = ', '.join(sorted(locations))
            batch.append(f"{word:<20}: se encuentra en el documento {locations_str}\n")

            if len(batch) >= batch_size:
                out_file.writelines(batch)
                batch.clear()

        if batch:
            out_file.writelines(batch)


def main(directory_path, output_file):
    start_time = time.time()

    file_paths = [os.path.join(directory_path, f) for f in os.listdir(directory_path) if f.endswith('.txt')]

    merged_results = defaultdict(set)

    with ProcessPoolExecutor() as executor:
        for result in executor.map(process_file_chunk, file_paths):
            for word, locations in result.items():
                merged_results[word].update(locations)

    write_results(merged_results, output_file)

    end_time = time.time()
    print(f"Tiempo de ejecución: {end_time - start_time:.2f} segundos")


if __name__ == "__main__":
    directory_path = "C:/Users/harry/Documents/files1"
    output_file = "C:/Users/harry/Documents/resultados/res.txt"
    main(directory_path, output_file)


