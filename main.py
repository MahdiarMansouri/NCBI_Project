from datetime import datetime
from model.DB.db_model import DB
from model.entity.blast_model import BLAST


start_time = datetime.now()

# Database information:
db_info = {
    'host': 'localhost',
    'user': 'root',
    'password': 'root123',
    'database': 'wgs'
}

WGS = "combined_wgs.fasta"
Gene = "mepa"

db = DB(Gene, db_info)

# Create 'combined_wgs.fasta' file for blast
genomes = db.search_all_genomes()
db.create_combined_wgs([i for i in range(1, len(genomes))])

# Create a list of genes from database for blast
genes_list = db.search_all_genes()

# Loop through genes and blast and create table of each one and export excel from the results
for gene in genes_list:
    print('-' * 20)
    print('name: ', gene.name)
    blast = BLAST(WGS, gene)
    blast.blast()
    db = DB(gene.name, db_info)
    db.create_and_insert_blast_results(gene.name, gene.name)
    db.add_cutoff_column(gene.name)
    db.export_table(gene.name, gene.name, 'excel')



end_time = datetime.now()
print()
print('Duration: {}'.format(end_time - start_time))
