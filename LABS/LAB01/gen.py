import os
import random

# Target file size: 20GB in bytes
target_size = 0.05 * 1024 * 1024 * 1024  # 20GB
output_file = "20_2GB.txt"

# Load word list (use system dictionary or download one)
word_list_file = "words.txt"  # Common on Linux/macOS
if not os.path.exists(word_list_file):
    # Fallback: use a small built-in list or prompt to download
    words = [
        "apple", "banana", "cat", "dog", "elephant", "fish", "grape", "house",
        "ice", "jungle", "kite", "lion", "moon", "nest", "orange", "pen",
        "queen", "river", "sun", "tree", "umbrella", "violet", "water", "xray",
        "yellow", "zebra"
    ]
    print("Warning: Dictionary file not found. Using small built-in word list.")
else:
    with open(word_list_file, 'r', encoding='utf-8') as f:
        words = [line.strip() for line in f if line.strip()]

# Ensure we have words to work with
if not words:
    raise ValueError(
        "Word list is empty. Please provide a valid word list file.")

# Generate the file
with open(output_file, 'w') as f:
    current_size = 0
    chunk_size = 10 * 1024 * 1024  # Process 1MB chunks (in terms of words)
    # Avg word length ~5-10 bytes, adjust as needed
    words_per_chunk = chunk_size // 10

    while current_size < target_size:
        # Generate a chunk of random words
        chunk = ' '.join(random.choices(words, k=words_per_chunk))
        f.write(chunk + '\n')
        f.flush()  # Ensure data is written to disk
        current_size = os.path.getsize(output_file)

        # Optional: Print progress
        if current_size % (1024 * 1024 * 100) == 0:  # Every 100MB
            print(f"Progress: {current_size / (1024*1024*1024):.2f}GB")

print(
    f"Generated {output_file} with size ~{current_size / (1024*1024*1024):.2f}GB")
