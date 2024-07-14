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

WGS = 'combined.fasta'
Gene = 'mepA'

# run functions for testing:
db = DB(Gene, db_info)
db.search_genome_by_id(3)

blast = BLAST(WGS, Gene)
blast.blast()

db.save()
db.add_cutoff_column(Gene)
#db.show_database_contents(Gene)

end_time = datetime.now()
print()
print('Duration: {}'.format(end_time - start_time))
