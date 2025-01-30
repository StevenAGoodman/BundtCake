import pandas as pd
import gzip
from io import BytesIO
import urllib.request
import re
import json
import ftplib
from itertools import groupby
import math

start_idx = 0
global last_idx
last_idx = start_idx

# infile
    # Assembly Accession	Assembly Name	Organism Name	Annotation Count Gene Total
genome_id_file = "./genome_ids.tsv"
genome_id_df = pd.read_csv(genome_id_file, sep="\t")
genome_id_df = genome_id_df[genome_id_df["Assembly Accession"].str.startswith("GCF")]
genome_id_df = genome_id_df.sample(frac=1)
genome_id_df = genome_id_df.reset_index()
accession_list = genome_id_df["Assembly Accession"].tolist()

# outfile
    # assembly_accession:{genes[], organism, gene_total}
#### genome_seq_file = "./sample_genomes.json"
#### genome_seq_file = open(genome_seq_file, "a")
#### genome_seq_file.write("{\n")
genome_seq_file = "./genome_seqs.txt"
genome_seq_file = open(genome_seq_file, "a")
genome_seq_file.write("################ SESSION START ################\n")

while last_idx < len(genome_id_df):
    # ncbi ftp server
    ftp = ftplib.FTP("ftp.ncbi.nlm.nih.gov")
    ftp.login()
    ftp.cwd("/genomes/all/GCF")

    def getGreatestLastDigit(array: list, version: int):

        s = f"^.*((v{version})|(\.{version})).*$"
        l = list(filter(re.compile(s).match, array))
        print("\tfiltered: ", l)
        if len(l) == 1:
            return l[0]
        else:
            return l[max(range(len(l)), key=lambda x: int(re.search("^GCF_\\d+\\.(\\d)_.+$", l[x]).group(1)))]

    def getFTPGenes(accession):
        dir = f"{accession[4:7]}/{accession[7:10]}/{accession[10:13]}" # GCF_000195955.2_ASM19595v2/GCF_000195955.2_ASM19595v2_genomic.gtf.gz
        ftp.cwd(dir)
        print(dir, ftp.nlst())
        subdir = getGreatestLastDigit(ftp.nlst(), accession[-1]) if len(ftp.nlst()) != 1 else ftp.nlst()[0]
        ftp.cwd(subdir)
        file = list(filter(re.compile("^.+_feature_table\.txt\.gz$").match, ftp.nlst()))[0]

        url = f"https://ftp.ncbi.nlm.nih.gov/genomes/all/GCF/{dir}/{subdir}/{file}"

        with urllib.request.urlopen(url) as response:
            content = response.read()

        content = gzip.decompress(content)
        content = content[content.find(b"\nNC_")+1:-4]
        genome_df = pd.read_csv(BytesIO(content), sep="\t")
        # names=["feature", "class", "assembly", "assembly_unit", "seq_type", "chromosome", "genomic_accession", "start", "end", "strand", "product_accession", "non-redundant_refseq", "related_accession", "name", "symbol", "GeneID", "locus_tag", "feature_interval_length", "product_length", "attributes"]
        #### gene_symbols = genome_df[genome_df["# feature"]=="gene"]["symbol"].tolist()
        #### gene_symbols = [x if type(x) == str else str(x) for x in gene_symbols]
        gene_ids = genome_df[genome_df["# feature"]=="CDS"]["product_accession"].tolist()

        ftp.cwd("../../../..")
        return gene_ids
        
    def writeInfo(accession): #### organism, total_genes, lastItem=False):
        #### main_dict = {}
        gene_ids = getFTPGenes(accession)
        #### main_dict["organism"] = organism
        #### main_dict["total genes"] = total_genes
        #### main_dict["gene symbols"] = gene_symbols
        gene_ids = [item for item in gene_ids if type(item) == str]

        genome_seq_file.write(' '.join(gene_ids) + "\n")

        #### print(f"{total_genes} || {len(gene_symbols)}")


    for idx in range(len(accession_list)): #### for idx, row in genome_id_df.iloc[last_idx:].iterrows():
        try:
            if idx == len(genome_id_df):
                writeInfo(accession_list[idx]) #### row["Assembly Accession"], row["Organism Name"], int(row["Annotation Count Gene Total"]
            else:
                writeInfo(accession_list[idx])
            last_idx = idx 
            print("Last finished index: ", last_idx) 
        except Exception as e:
            print(e)
            print(type(e).__name__)
            last_idx = idx
            print("Last finished index: ", last_idx) 

            if type(e).__name__ == "EOFError" or type(e).__name__ == "TimeoutError":   
                break
            else:
                assert 1 == 0 
    continue

