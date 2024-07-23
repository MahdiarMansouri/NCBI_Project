from datetime import datetime
from model.DB.db_model import DB
from model.entity.blast_model import BLAST
from model.entity.duplicate import *
from model.entity.analysis import *


start_time = datetime.now()

# Database information:
db_info = {
    'host': 'localhost',
    'user': 'root',
    'password': '@danialgh1313',
    'database': 'wgs'
}

WGS = r"combined_wgs.fasta"
Gene = "mepa"

db = DB(Gene, db_info)
db.create_combined_wgs()

# Create a list of genes from database for blast
genes_list = db.search_all_genes()
output_file = 'gene_analysis_results.xlsx'
print(genes_list)
# Loop through genes and blast and create table of each one and export excel from the results
for gene in genes_list:
    print('-' * 20)
    print('name: ', gene.name)
    blast = BLAST(WGS, gene)
    blast.blast()
    db = DB(gene.name, db_info)
    db.create_and_insert_blast_results(name_list, gene.name, gene.name)
    db.add_cutoff_column(gene.name)
    duplicate_checker = DuplicateCheck(gene.name, db_info)
    duplicate_checker.process_duplicates()
    analysis = Analysis(db_info)
    analysis.process_analysis(output_file)
    db.export_table(gene.name, gene.name, 'excel')



end_time = datetime.now()
print()
print('Duration: {}'.format(end_time - start_time))