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
blast = BLAST(WGS, Gene)
db = DB(Gene, db_info)

# gene = db.search_genome_by_id(2)
# print(gene)
# for genome in genomes_list:
#     print(genome)

# db.show_database_contents('mepa')

# db.create_combined_wgs([i for i in range(1, 10)])

blast.blast()

db.save()
db.add_cutoff_column(Gene)
#db.show_database_contents(Gene)

end_time = datetime.now()
print()
print('Duration: {}'.format(end_time - start_time))
