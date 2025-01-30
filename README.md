# BundtCake

Plan:
- use gene2go: implement in ncbigenomes.py 
    - output gene_seqs (go annotations with whitespace) 
    - output nt_seqs (with white between genes; ORDER MATTERS)
- run it (hopefully it finishes fast!)
- while its running, configure the two transformers
    - ???????????????????????????????????????????????????????
- determine evaluation metrics
    - just compare overall accuracy
        - mse, rmse, mape, etc (https://stackoverflow.com/questions/75552538/how-to-compare-two-different-models)
    - analyze genome realism
        - presence of metabolic network similarities to real genomes
    - continuation laws
