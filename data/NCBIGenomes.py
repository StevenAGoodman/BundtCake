import pandas as pd
import gzip
from io import BytesIO
import urllib.request
import re
import json
import ftplib
from itertools import groupby
import math

start_idx = 21257
global last_idx
last_idx = start_idx

print("gene count: expected || actual")
total_mismatches = 0

# infile
    # Assembly Accession	Assembly Name	Organism Name	Annotation Count Gene Total
infile = "./ref_genome_info.tsv"
info_df = pd.read_csv(infile, sep="\t")
info_df = info_df[info_df["Assembly Accession"].str.startswith("GCF")]
info_df = info_df.reset_index()

# outfile
    # assembly_accession:{genes[], organism, gene_total}
outpath = "./ref_genomes.json"
outfile = open(outpath, "a")
outfile.write("{\n")

while last_idx < len(info_df):
    # ncbi ftp server
    ftp = ftplib.FTP("ftp.ncbi.nlm.nih.gov")
    ftp.login()
    ftp.cwd("/genomes/all/GCF")

    def all_equal(iterable):
        g = groupby(iterable)
        return next(g, True) and not next(g, False)

    def getGreatestLastDigit(array: list, version: int):
        # array = [x for x in array if x[-1].isdigit()==True]
        # ma = list(map(lambda x: int(x[-1]), array))
        # if all_equal(ma):
        s = f"^.*((v{version})|(\.{version})).*$"
        l = list(filter(re.compile(s).match, array))
        print("\tfiltered: ", l)
        if len(l) == 1:
            return l[0]
        else:
            return l[max(range(len(l)), key=lambda x: int(re.search("^GCF_\\d+\\.(\\d)_.+$", l[x]).group(1)))]
        # else:
        #     return array[max(range(len(ma)), key=ma.__getitem__)]

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
        genome_df = genome_df[genome_df["# feature"]=="gene"]
        gene_symbols = genome_df["symbol"].tolist()
        gene_symbols = [x if type(x) == str else str(x) for x in gene_symbols]
        gene_ids = genome_df["GeneID"].tolist()

        # genes = []
        # for idx, row in genome_df.iterrows():
        #     try:
        #         genes.append(re.search(";Name=([a-zA-Z]+);gbkey", row["description"]).group(1))
        #     except:
        #         pass

        ftp.cwd("../../../..")
        return gene_symbols, gene_ids
        
    def writeInfo(accession, organism, total_genes, lastItem=False):
        main_dict = {}
        gene_symbols, gene_ids = getFTPGenes(accession)
        main_dict["organism"] = organism
        main_dict["total genes"] = total_genes
        main_dict["gene symbols"] = gene_symbols
        main_dict["gene ids"] = gene_ids

        print(f"{total_genes} || {len(gene_symbols)}")
        global total_mismatches
        total_mismatches += 1 if total_genes != len(gene_symbols) else 0

        if lastItem:
            outfile.write(f'"{accession}": ' + json.dumps(main_dict))
        else:
            outfile.write(f'"{accession}": ' + json.dumps(main_dict) + ",\n")

    for idx, row in info_df.iloc[last_idx:].iterrows():
        try:
            if idx == len(info_df):
                writeInfo(row["Assembly Accession"], row["Organism Name"], int(row["Annotation Count Gene Total"]), True)
            else:
                writeInfo(row["Assembly Accession"], row["Organism Name"], int(row["Annotation Count Gene Total"]))
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


outfile.write("}")
print(total_mismatches)