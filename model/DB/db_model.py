import pandas as pd
import mysql.connector
from ..entity.gene import *
from ..entity.genome import WholeGenome


class DB:
    def __init__(self, gene, db_info):
        self.gene = gene
        self.db_info = db_info
        self.mydb = None

    def connect(self):
        try:
            self.mydb = mysql.connector.connect(
                host=self.db_info['host'],
                user=self.db_info['user'],
                passwd=self.db_info['password'],
                database=self.db_info['database']
            )
            self.cursor = self.mydb.cursor()
            print("Successfully connected to the database.")
        except mysql.connector.Error as err:
            print(f"Error: {err}")

    def disconnect(self, commit=False):
        if commit:
            self.mydb.commit()
        self.cursor.close()
        self.mydb.close()

    def create_and_insert_blast_results(self, table_name, csv_file):
        # Define table columns and types
        columns = '''
            id INT AUTO_INCREMENT PRIMARY KEY,
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
            sbjct_frame INT,
            qseq_path VARCHAR(300),
            sseq_path VARCHAR(300)
        '''

        create_table_query = f"CREATE TABLE IF NOT EXISTS {table_name} ({columns})"
        insert_query = f"""
            INSERT INTO {table_name} (query_id, subject_id, identity, alignment_length,
                                      mismatches, gap_opens, q_start, q_end, s_start, s_end, evalue, bit_score,
                                      query_length, subject_length, subject_strand, query_frame, sbjct_frame, qseq_path, sseq_path)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        self.connect()
        self.cursor.execute(create_table_query)

        # Read the CSV file
        df = pd.read_csv(csv_file, header=None)

        # Iterate through each row in the DataFrame
        for idx, row in df.iterrows():
            # Define paths for qseq and sseq files
            qseq_path = f"{self.gene}_qseq_{idx}.txt"
            sseq_path = f"{self.gene}_sseq_{idx}.txt"

            # Write sequences to files
            with open(qseq_path, 'w') as qf:
                qf.write(row[17])
            with open(sseq_path, 'w') as sf:
                sf.write(row[18])

            # Insert row into the table

            row_data = tuple(row[:17]) + (qseq_path, sseq_path)
            self.cursor.execute(insert_query, row_data)

        # Disconnect from the database
        self.disconnect(commit=True)

    def add_cutoff_column(self, table_name):
        self.connect()
        alter_table_query = f"ALTER TABLE {table_name} ADD COLUMN cutoff TINYINT"
        update_query = f"""UPDATE {table_name} SET cutoff = CASE
                        WHEN identity < 85 OR (alignment_length / query_length) * 100 < 90 THEN 0
                        ELSE 1
                    END
                """
        self.cursor.execute(alter_table_query)
        self.cursor.execute(update_query)
        print(f"Column 'cutoff' added to {table_name} table.")
        self.disconnect(commit=True)

    def show_database_contents(self, table_name):
        # Query to select all rows from the specified table
        self.connect()
        select_query = f"SELECT * FROM {table_name}"

        # Execute the query
        df = pd.read_sql_query(select_query, self.mydb, index_col='id')

        # To show all columns
        # pd.set_option('display.max_columns', None)

        self.disconnect()
        return df

    def execute_command(self, sql_command):
        self.connect()
        self.cursor.execute(sql_command)
        self.disconnect()

    def save(self):
        table_name = self.gene
        csv_file = f"{self.gene}.csv"
        self.create_and_insert_blast_results(table_name, csv_file)

    def search_result_table_by_name(self, table_name):
        self.connect()
        self.cursor.execute(f"SELECT * FROM {table_name}")
        rows = self.cursor.fetchall()
        self.disconnect()
        return rows

    def add_row(self, table_name, row_data):
        self.connect()
        insert_query = f"""
                INSERT INTO {table_name} (query_id, subject_id, identity, alignment_length, 
                mismatches, gap_opens, q_start,q_end, s_start, s_end, evalue, bit_score, query_length, 
                subject_length, subject_strand, query_frame, sbjct_frame, qseq_path, sseq_path)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s )
            """
        self.cursor.execute(insert_query, row_data)
        self.disconnect(commit=True)

    def delete_row_from_result_table_by_condition(self, table_name, condition):
        self.connect()
        self.cursor.execute(f" DELETE FROM {table_name} WHERE {condition}")
        self.disconnect(commit=True)

    def update_result_table_row_by_condition(self, table_name, updates, condition):
        self.connect()
        self.cursor.execute(f"UPDATE {table_name} SET {updates} WHERE {condition}")
        self.disconnect(commit=True)

    def search_result_table_by_name(self, table_name):
        self.connect()
        query = """
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = %s 
                AND table_name LIKE %s
        """

        self.cursor.execute(query, (self.db_info['database'], table_name))
        rows = self.cursor.fetchall()
        self.disconnect()
        return rows

    def search_gene_by_name(self, gene_name):
        self.connect()
        self.cursor.execute("SELECT * FROM genes_sample_files WHERE file_name LIKE %s", [f'%{gene_name}%'])
        genes = self.cursor.fetchall()
        genes_list = []
        for gene in genes:
            gene = Gene(*gene)
            genes_list.append(gene)

        self.disconnect()
        return genes_list

    def search_gene_by_id(self, id):
        self.connect()
        self.cursor.execute("SELECT * FROM genes_sample_files WHERE id=%s", [id])
        gene = self.cursor.fetchone()
        gene = Gene(*gene)
        self.disconnect()
        return gene

    def search_genome_by_name(self, genome_name):
        self.connect()
        self.cursor.execute("SELECT * FROM genomes_sample_files WHERE file_name LIKE %s", [f'%{genome_name}%'])
        genomes = self.cursor.fetchall()
        genomes_list = []
        for genome in genomes:
            genome = WholeGenome(*genome)
            genomes_list.append(genome)

        self.disconnect()
        return genomes_list

    def search_genome_by_id(self, id):
        self.connect()
        self.cursor.execute("SELECT * FROM genomes_sample_files WHERE id=%s", [id])
        genome = self.cursor.fetchone()
        genome = WholeGenome(*genome)
        self.disconnect()
        return genome

    def export_CSV(self, table_name, output_file):
        self.connect()
        select_table = f"SELECT * FROM {table_name}"
        self.cursor.execute(select_table)
        rows = self.cursor.fetchall()
        columns = []
        for desc in self.cursor.description:
            columns.append(desc[0])

        self.disconnect()
        df = pd.DataFrame(rows, columns=columns)
        df.to_csv(output_file, index=False)

    def create_combined_wgs(self, id_list):
        self.connect()
        output_file = f"combined_wgs.fasta"
        file_contents = []
        for id in id_list:
            genome = self.search_genome_by_id(id)

            with open(genome.file_path, 'r') as file:
                lines = file.readlines()
                lines_with_newline = [line.rstrip('\n') + '\n' for line in lines]
                for line in lines_with_newline:
                    file_contents.append(line)

        with open(output_file, 'w') as file:
            file.writelines(file_contents)







