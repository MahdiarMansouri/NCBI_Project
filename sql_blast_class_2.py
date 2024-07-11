import subprocess
from Bio import SeqIO
import os
from datetime import datetime
import pandas as pd
import mysql.connector

class BLAST:
    def __init__(self, WGS, gene, db_info):
        self.WGS = WGS
        self.gene = gene
        self.db_info = db_info
        self.mydb = None

    def blast(self):
        # Create BLAST database
        blast_dir = "C:\\Program Files\\NCBI\\blast-2.15.0+\\bin"
        os.chdir(blast_dir)

        result = subprocess.run(["makeblastdb", "-version"], capture_output=True, text=True)
        print("makeblastdb version output:", result.stdout)
        if result.stderr:
            print("makeblastdb version error:", result.stderr)

        result = subprocess.run(["makeblastdb", "-in", self.WGS, "-dbtype", "nucl", "-out", "WGS"], capture_output=True,
                                text=True)
        print("makeblastdb output:", result.stdout)
        if result.stderr:
            print("makeblastdb error:", result.stderr)

        # Perform BLAST search
        result = subprocess.run(
            ["blastn", "-query", f"{self.gene}.fasta", "-db", "WGS", "-out", f"{self.gene}.csv", "-outfmt",
             "10 qseqid sseqid pident length mismatch gapopen qstart qend sstart send evalue bitscore qlen slen sstrand qframe sframe"],
            capture_output=True, text=True
        )
        print("blastn output:", result.stdout)
        if result.stderr:
            print("blastn error:", result.stderr)

    def connect(self):
        # Connect to MySQL server
        self.mydb = mysql.connector.connect(
            host=self.db_info['host'],
            user=self.db_info['user'],
            passwd=self.db_info['passwd'],
            database=self.db_info['database']
        )

    def create_table(self, table_name):
        # Define table columns and types
        columns = '''
            query_id VARCHAR(100),
            subject_id VARCHAR(100),
            identity FLOAT,
            alignment_length INT,
            mismatches INT,
            gap_opens INT,
            q_start INT,
            q_end INT,
            s_start INT,
            s_end INT,
            evalue FLOAT,
            bit_score FLOAT,
            query_length INT,
            subject_length INT,
            subject_strand VARCHAR(20),
            query_frame INT,
            sbjct_frame INT
        '''
        create_table_query = f"CREATE TABLE IF NOT EXISTS {table_name} ({columns})"
        self.execute(create_table_query)

    def insert_data_from_csv(self, table_name, csv_file):
        # Read the CSV file
        df = pd.read_csv(csv_file, header=None)

        cursor = self.mydb.cursor()

        insert_query = f"""
            INSERT INTO {table_name} (query_id, subject_id, identity, alignment_length,
                                      mismatches, gap_opens, q_start, q_end, s_start, s_end, evalue, bit_score, query_length, subject_length, subject_strand, query_frame, sbjct_frame)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        for _, row in df.iterrows():
            cursor.execute(insert_query, tuple(row))  # Assuming all columns in CSV align with insert query

        self.mydb.commit()
        cursor.close()

    def execute(self, sql):
        cursor = self.mydb.cursor()
        cursor.execute(sql)
        self.mydb.commit()
        cursor.close()

    def save(self):
        table_name = self.gene
        csv_file = f"{self.gene}.csv"
        self.insert_data_from_csv(table_name, csv_file)

    def show_database_contents(self, table_name):
        # Query to select all rows from the specified table
        select_query = f"SELECT * FROM {table_name}"

        # Execute the query
        cursor = self.mydb.cursor()
        cursor.execute(select_query)

        # Fetch all rows from the result set
        rows = cursor.fetchall()

        # Print column headers
        print("Database Contents:")
        print("-------------------")
        print(
            "query_id\tsubject_id\tidentity\talignment_length\tmismatches\tgap_opens\tq_start\tq_end\ts_start\ts_end\tevalue\tbit_score\tquery_length\tsubject_length\tsubject_strand\tquery_frame\tsbjct_frame")

        # Print each row
        for row in rows:
            print("\t".join(str(col) for col in row))

        cursor.close()

    def add_row(self, table_name, row_data):
        cursor = self.mydb.cursor()
        insert_query = f"""
            INSERT INTO {table_name} (query_id, subject_id, identity, alignment_length, mismatches, gap_opens, q_start, q_end, s_start, s_end, evalue, bit_score, query_length, subject_length, subject_strand, query_frame, sbjct_frame)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s )
        """
        cursor.execute(insert_query, row_data)
        self.mydb.commit()
        cursor.close()

    def delete_row(self, table_name, condition):
        cursor = self.mydb.cursor()
        delete = f""" DELETE FROM {table_name} WHERE {condition} 
        """
        cursor.execute(delete)
        self.mydb.commit()
        cursor.close()

    def update_row(self, table_name, updates, condition):
        cursor = self.mydb.cursor()
        update = f"UPDATE {table_name} SET {updates} WHERE {condition}"
        cursor.execute(update)
        self.mydb.commit()
        cursor.close()

    def gene_search_body(self, gene_name):
        cursor = self.mydb.cursor()
        cursor.execute("SHOW TABLES")
        tables = cursor.fetchall()

        table_names = []
        for table in tables:
            table_name = table[0]
            table_names.append(table_name)

        if gene_name in table_names:
            search_query = f"SELECT * FROM {gene_name}"
            cursor.execute(search_query)
            rows = cursor.fetchall()

            # Print column headers
            print("Database Contents:")
            print("-------------------")
            print(
                "query_id\tsubject_id\tidentity\talignment_length\tmismatches\tgap_opens\tq_start\tq_end\ts_start\ts_end\tevalue\tbit_score\tquery_length\tsubject_length\tsubject_strand\tquery_frame\tsbjct_frame")
        else:
            print(f"No table found for gene: {gene}")
            rows = []

        cursor.close()
        return rows

    def gene_search(self, gene_name):
        result_rows = gene.gene_search_body(gene_name)
        for row in result_rows:
            print(row)

    def search_by_genome(self):
        pass

    def search_by_gene_and_genome(self):
        pass

    def export_CSV(self, table_name, output_file):
        cursor = self.mydb.cursor()
        select_table = f"SELECT * FROM {table_name}"
        cursor.execute(select_table)
        rows = cursor.fetchall()
        columns = []
        for desc in cursor.description:
            columns.append(desc[0])

        cursor.close()
        df = pd.DataFrame(rows, columns=columns)
        df.to_csv(output_file, index=False)

    def close_connection(self):
        if self.mydb:
            self.mydb.close()


start_time = datetime.now()

# information input:
db_info = {
    'host': 'localhost',
    'user': 'root',
    'passwd': 'root123',
    'database': 'blast'
}

WGS = "combined.fasta"
Gene = "mepa"

####run functions for testing:
gene = BLAST(WGS, Gene, db_info)
gene.blast()
gene.connect()
gene.create_table(Gene)
gene.save()
gene.show_database_contents(Gene)

gene = BLAST(WGS, Gene, db_info)
gene.connect()

# for testing the add_row function:
# new_row = ("query1", "0001", 99.9, 100, 0, 0, 1, 100, 1, 100, 1e-50, 200, 100, 100, "plus", 1, 1)
# gene.add_row(Gene,new_row)

# for testing the delete_row funxtion:
# gene.delete_row(Gene, "query_id = 'query1'")

# for testing the gene search method:
# gene.gene_search("mepa")

# for testing update_row function:
# gene.update_row("mepa", "identity = 30", "subject_id = '131598'")

# for testing the export_csv function:
# gene.export_CSV(Gene, f"{Gene}.CSV")

gene.show_database_contents(Gene)

end_time = datetime.now()
print()
print('Duration: {}'.format(end_time - start_time))
