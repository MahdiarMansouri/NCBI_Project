from datetime import datetime
from model.DB.db_model import DB
from model.entity.blast_model import BLAST


start_time = datetime.now()

# information input:
db_info = {
    'host': 'localhost',
    'user': 'root',
    'password': 'mrnd181375',
    'database': 'wgs'
}

WGS = "combined.fasta"
Gene = "mepa"

# run functions for testing:
blast = BLAST(WGS, Gene)
db = DB(Gene, db_info)

blast.blast()
#
db.save()
#db.show_database_contents(Gene)

end_time = datetime.now()
print()
print('Duration: {}'.format(end_time - start_time))
