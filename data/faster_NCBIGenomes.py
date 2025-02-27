import pandas as pd
import gzip
from io import BytesIO
import urllib.request
import re
import ftplib
from concurrent.futures import ThreadPoolExecutor
import os


start_idx = 13950
global last_idx
last_idx = start_idx

# Input file
genome_id_file = "./genome_ids.tsv"
genome_id_df = pd.read_csv(genome_id_file, sep="\t")
genome_id_df = genome_id_df[genome_id_df["Assembly Accession"].str.startswith("GCF")]
accession_list = genome_id_df["Assembly Accession"].tolist()

# Output files
nt_seq_file = "./nt_seqs.txt"
nt_f = open(nt_seq_file, "a")
nt_f.write("################ SESSION START ################\n")

gene_seq_file = "./gene_seqs.txt"
gene_f = open(gene_seq_file, "a")
gene_f.write("################ SESSION START ################\n")

# info files
gene_info_f = "./all.gene_info"
gene_info_df = pd.read_csv(gene_info_f, sep="\t", usecols=["GeneID", "Symbol"])
gene_info_df["GeneID"] = gene_info_df['GeneID'].astype(str) 
id2sym = pd.Series(gene_info_df.Symbol.values, index=gene_info_df.GeneID).to_dict()

# genetogo = "./gene2go" 
# genetogo = open(genetogo, "r")

# Pre-compiled regex
cds_file_regex = re.compile("^.+_cds_from_genomic\.fna\.gz$")
subdir_version_regex = re.compile("^GCF_\d+\.(\d)_.+$")

# Get the latest version of the directory
def get_latest_version(array, version):
    s = f"^.*((v{version})|(\.{version})).*$"
    l = list(filter(re.compile(s).match, array))
    if len(l) == 1:
        return l[0]
    return l[max(range(len(l)), key=lambda x: int(subdir_version_regex.search(l[x]).group(1)))]

def get_associated_go(gene_id):
    for line in gene_info_f:
        if gene_id in line:
            return line.split("\t")[5]
    return None

def get_gene_symbol(gene_id):
    return id2sym.get(gene_id, None)

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
        file_name = list(filter(cds_file_regex.match, files))[0]
        url = f"https://ftp.ncbi.nlm.nih.gov/genomes/all/GCF/{dir_path}/{subdir}/{file_name}"

        with urllib.request.urlopen(url) as response:
            content = gzip.decompress(response.read())
        content: list[str] = content.decode("utf-8").split("\n")

        gene_ids = []
        nt_seq = ""
        missed_gene = 0
        for line in content:    
            # gene ids
            if line.startswith(">"):
                symbol = re.search("\\[gene=([a-zA-Z]+)\\] \\[", line)
                if symbol:
                    gene_ids.append(symbol.group(1))
                    nt_seq += " " # split genes by whitespace
                else:
                    gene_id = re.search("GeneID:(\\d+)", line)
                    # if not gene_id:
                    #     gene_id = re.search("\\[protein_id=([A-Z]+_\\d+\\.\\d)\\]", line)
                    if gene_id:
                        symbol = get_gene_symbol(gene_id.group(1))
                        if symbol: 
                            gene_ids.append(symbol) 
                            nt_seq += " " # split genes by whitespace
                        else: 
                            gene_id = re.search("\\[locus_tag=([a-zA-Z\\d]+_[a-zA-Z\\d]+)\\] \\[", line)
                            if gene_id:
                                gene_ids.append(gene_id.group(1))
                                nt_seq += " " # split genes by whitespace
                            else:
                                missed_gene+=1
        
            # nt seq
            else:
                nt_seq += line
 
        return gene_ids, nt_seq, missed_gene

    except Exception as e:
        print(f"Error fetching genes for {accession}: {e}")

        return [], "",0
    finally:
        ftp.quit()

# Write results
def process_accession(accession):
    gene_ids, nt_seq, missed_genes = fetch_genes(accession)
    gene_f.write(f"{accession}: {' '.join(gene_ids)}\n" if gene_ids else "")
    nt_f.write(f"{accession}: {nt_seq}\n" if nt_seq else "")
    print("Last finished index: ", accession_list.index(accession), "Missed genes: ", missed_genes) 


# Parallel processing
def main():
    # for accession in accession_list[start_idx:]:
    #     process_accession(accession)
    with ThreadPoolExecutor(max_workers=os.cpu_count() + 5) as executor:
        results = executor.map(process_accession, accession_list[start_idx:])

if __name__ == "__main__":
    main()