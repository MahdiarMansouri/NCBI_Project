from datetime import datetime
from model.DB.db_model import DB
from model.entity.blast_model import BLAST


start_time = datetime.now()

# information input:
db_info = {
    'host': 'localhost',
    'user': 'root',
    'password': 'root123',
    'database': 'wgs'
}

WGS = "combined_wgs.fasta"
Gene = "mepa"

# run functions for testing:
db = DB(Gene, db_info)

genomes = db.search_all_genomes()
db.create_combined_wgs([i for i in range(1, len(genomes))])

genes_list = db.search_all_genes()

# for gene in genes_list:
#     blast = BLAST(WGS, gene.file_path)
#     blast.blast()



# gene = db.search_genome_by_id(1)
# print(gene)
# for genome in genomes_list:
#     print(genome)

# db.show_database_contents('mepa')


# db.export_table('mepa', 'mepa', 'excel')


#
# db.save()
# db.add_cutoff_column(Gene)
#db.show_database_contents(Gene)

end_time = datetime.now()
print()
print('Duration: {}'.format(end_time - start_time))
