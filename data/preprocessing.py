# operon preprocessing
#   operon grouping => json file

import urllib.request
import pandas as pd
import ftplib
import urllib
import gzip
from io import BytesIO

def getOperonGrouping(genome_id):
    # get ftp file
    url = f"https://ftp.ncbi.nlm.nih.gov/genomes/all/GCF/{dir}/{subdir}/{file}"

    with urllib.request.urlopen(url) as response:
        content = response.read()

    content = gzip.decompress(content)
    content = content[content.find(b"\nNC_")+1:-4]
    genome_df = pd.read_csv(BytesIO(content), sep="\t")
    # names=["feature", "class", "assembly", "assembly_unit", "seq_type", "chromosome", "genomic_accession", "start", "end", "strand", "product_accession", "non-redundant_refseq", "related_accession", "name", "symbol", "GeneID", "locus_tag", "feature_interval_length", "product_length", "attributes"]
    genome_df = genome_df[genome_df["# feature"]=="gene"]

def main(outpath):
    return outpath

print(urllib.request.urlopen("https://huggingface.co/docs/transformers/main/llm_tutorial").text)
