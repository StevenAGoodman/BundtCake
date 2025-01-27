import pandas as pd
import gzip
from io import BytesIO
import urllib.request
import re
import ftplib
from concurrent.futures import ThreadPoolExecutor
import os

start_idx = 0
global last_idx
last_idx = start_idx

# Input file
genome_id_file = "./genome_ids.tsv"
genome_id_df = pd.read_csv(genome_id_file, sep="\t")
genome_id_df = genome_id_df[genome_id_df["Assembly Accession"].str.startswith("GCF")]
accession_list = genome_id_df["Assembly Accession"].tolist()

# Output file
genome_seq_file = "./genome_seqs.txt"
f = open(genome_seq_file, "a")
f.write("################ SESSION START ################\n")

# Pre-compiled regex
feature_table_regex = re.compile("^.+_feature_table\.txt\.gz$")
subdir_version_regex = re.compile("^GCF_\\d+\\.(\\d)_.+$")

# Get the latest version of the directory
def get_latest_version(array, version):
    s = f"^.*((v{version})|(\.{version})).*$"
    l = list(filter(re.compile(s).match, array))
    if len(l) == 1:
        return l[0]
    return l[max(range(len(l)), key=lambda x: int(subdir_version_regex.search(l[x]).group(1)))]

# Fetch genes from FTP
def fetch_genes(accession):
    ftp = ftplib.FTP("ftp.ncbi.nlm.nih.gov")
    ftp.login()
    ftp.cwd("/genomes/all/GCF")

    try:
        dir_path = f"{accession[4:7]}/{accession[7:10]}/{accession[10:13]}"
        ftp.cwd(dir_path)
        subdirs = ftp.nlst()
        print(dir_path, subdirs)
        subdir = get_latest_version(subdirs, accession[-1]) if len(subdirs) != 1 else subdirs[0]
        ftp.cwd(subdir)

        files = ftp.nlst()
        file_name = list(filter(feature_table_regex.match, files))[0]
        url = f"https://ftp.ncbi.nlm.nih.gov/genomes/all/GCF/{dir_path}/{subdir}/{file_name}"

        with urllib.request.urlopen(url) as response:
            content = gzip.decompress(response.read())
        
        content = content[content.find(b"\nNC_")+1:-4]
        genome_df = pd.read_csv(BytesIO(content), sep="\t")
        gene_ids = genome_df[genome_df["# feature"] == "CDS"]["product_accession"].dropna().tolist()
        return gene_ids

    except Exception as e:
        print(f"Error fetching genes for {accession}: {e}")
        return []
    finally:
        ftp.quit()

# Write results
def process_accession(accession):
    gene_ids = fetch_genes(accession)
    f.write(f"{accession}: {' '.join(gene_ids)}\n" if gene_ids else "")
    print("Last finished index: ", accession_list.index(accession)) 


# Parallel processing
def main():
    # for accession in accession_list[start_idx:]:
    #     process_accession(accession)
    with ThreadPoolExecutor(max_workers=os.cpu_count()) as executor:
        results = executor.map(process_accession, accession_list)

if __name__ == "__main__":
    main()
