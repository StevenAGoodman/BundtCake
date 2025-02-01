
# Input and output file names
input_file = "gene_seqs"
output_file = "cleaned_gene_seqs.txt"
output_file = open(output_file, "a")

for line in open(input_file, "r"):
    cleaned_ids = []
    line = line.replace("\n", "")
    for id in line.split(" "):
        cleaned_ids.append(id) if "_" not in id else None
    output_file.write(" ".join(cleaned_ids) + "\n")