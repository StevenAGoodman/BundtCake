from datasets import load_dataset
import tiktoken
from multiprocessing import Pool
import numpy as np
import os
import tqdm

def extract_unique(batch):
    print("yay@")
    unique_ids = set()
    for row in batch["text"]:
        unique_ids.update(row.split())  # Split whitespace-separated IDs
    return unique_ids

if __name__ == "__main__":
    dataset = load_dataset("Stegvean/CakeGenomes")

    print(dataset)

    # Process dataset in parallel using multiprocessing
    num_proc = 6  # Adjust based on your CPU cores
    print("e")
    with Pool(num_proc) as pool:
        results = pool.map(extract_unique, dataset["train"].iter(batch_size=1000))

    # Combine results to get the final unique set
    unique_gene_ids = set().union(*results)

    vocab_size = len(unique_gene_ids)
    print(vocab_size)

    # export tokenizer data
    import json
    json.dump(
        {"id_vocab": list(unique_gene_ids), "vocab_size": vocab_size}, 
        open("tokenizer_data_train.json", "w"), indent=3)