import subprocess
import os
from datetime import datetime
import pandas as pd
import mysql.connector

from model.DB.db_model import DB
from model.entity.gene import Gene





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
blast_gene = BLAST(WGS, Gene)
gene = DB(Gene, db_info)

blast_gene.blast()
gene.connect()
gene.create_table(Gene)
gene.save()
gene.show_database_contents(Gene)

end_time = datetime.now()
print()
print('Duration: {}'.format(end_time - start_time))
