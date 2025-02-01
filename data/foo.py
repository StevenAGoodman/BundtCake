import json

def generate_gene_go_json(input_file, output_file):
    gene_go_dict = {}
    
    with open(input_file, 'r') as file:
        for line in file:
            if line.startswith('#'):
                continue  # Skip comment lines
            parts = line.strip().split('\t')
            gene_id = parts[1]
            go_id = parts[2]
            
            if gene_id not in gene_go_dict:
                gene_go_dict[gene_id] = []
            if go_id not in gene_go_dict[gene_id]:
                gene_go_dict[gene_id].append(go_id)
    
    with open(output_file, 'w') as json_file:
        json.dump(gene_go_dict, json_file, indent=4)

# Example usage
input_file = './gene2go'  # Replace with the actual path to your gene2go file
output_file = 'gene_go_mapping.json'
generate_gene_go_json(input_file, output_file)
print(f'JSON file generated: {output_file}')