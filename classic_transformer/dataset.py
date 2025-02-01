"""
Main purpose: load in data set from external storage, tokenize it, pad it, etc
"""

from datasets import load_dataset
from multiprocessing import Pool
import numpy as np
import os
from tqdm import tqdm
import json

dataset = load_dataset("Stegvean/CakeGenomes")["reduced"]

# split dataset
split_dataset = dataset.train_test_split(test_size=0.03, shuffle=True)
split_dataset["val"] = split_dataset.pop('test') 
print(split_dataset)
print("training size:", len(split_dataset["train"]))
print("validation size:", len(split_dataset["val"]))

# seq lengths
get_len = lambda x: len(x.split(" "))
lens = list(map(get_len, dataset["text"]))
max_len = max(lens)

# start, end, and padding tokens
reduced_vocabulary = ["[PAD]", "[EOS]", "[SOS]"]
vocab_info = json.load(open("./tokenizer/tokenizer_data_train.json"))
reduced_vocabulary.extend(vocab_info["reduced_vocab"])
n = len(reduced_vocabulary)

id_to_int = { id:i for i,id in enumerate(reduced_vocabulary)}
int_to_id = { i:id for i,id in id_to_int.items()}
encode = lambda id_list: [id_to_int[id] for id in id_list]
decode = lambda int_list: [int_to_id[i] for i in int_list]

# padding and special tokens
def process(example):
    id_str = example#["text"]
    char_arr = ["[SOS]"]
    char_arr.extend(id_str.split(" "))
    char_arr.append("[EOS]")
    char_arr.extend(["[PAD]"] * (max_len - len(char_arr)))
    tokenized = encode(char_arr)
    return tokenized

def process(example):
    char_arr = ["[SOS]"] + example["text"].split(" ") + ["[EOS]"]
    char_arr += ["[PAD]"] * (max_len - len(char_arr))
    tokenized = encode(char_arr)
    return {'ids': tokenized, 'len': len(tokenized)}

if __name__ == "__main__":
    tokenized_dataset = split_dataset.map(
        process, 
        remove_columns=['text'], 
        num_proc=os.cpu_count(),  # Use all available CPU cores
        desc="Tokenizing dataset"
    )
    print(tokenized_dataset)

    # write to mem map
    for split, dset in tokenized_dataset.items():
        arr_len = np.sum(dset['len'], dtype=np.uint64)
        filename = os.path.join(os.path.dirname(__file__), 'data', f'{split}.bin')
        dtype = np.uint16 # (can do since enc.max_token_value == 50256 is < 2**16)
        arr = np.memmap(filename, dtype=dtype, mode='w+', shape=(arr_len,))
        total_batches = min(1024, len(dset))

        idx = 0
        for batch_idx in tqdm(range(total_batches), desc=f'writing {filename}'):
            # Batch together samples for faster write
            batch = dset.shard(num_shards=total_batches, index=batch_idx, contiguous=True).with_format('numpy')
            arr_batch = np.concatenate(batch['ids'])
            # Write into mmap
            arr[idx : idx + len(arr_batch)] = arr_batch
            idx += len(arr_batch)
        arr.flush()
