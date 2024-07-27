import mysql.connector
import subprocess
import os


class DuplicateCheck:
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
            print("Successfully connected to the database.")
            self.cursor = self.mydb.cursor()
        except mysql.connector.Error as err:
            print(f"Error: {err}")

    def disconnect(self, commit=False):
        if commit:
            self.mydb.commit()
        self.cursor.close()
        self.mydb.close()

    def column_exists(self, table_name, column_name):
        query = f"SHOW COLUMNS FROM {table_name} LIKE '{column_name}'"
        self.cursor.execute(query)
        result = self.cursor.fetchone()
        return result is not None

    def add_duplicate_column(self, table_name):
        if not self.column_exists(table_name, 'duplicate'):
            alter_table_query = f"ALTER TABLE {table_name} ADD COLUMN duplicate NVARCHAR(10) "
            try:
                self.cursor.execute(alter_table_query)
                print(f"Column 'duplicate' added to {table_name} table.")

                # Initialize all existing rows with 0
                update_query = f"UPDATE {table_name} SET duplicate = 1 WHERE cutoff = 1"
                self.cursor.execute(update_query)
                print("Initialized 'duplicate' column to 0 for all rows.")
                self.mydb.commit()
            except mysql.connector.Error as err:
                print(f"Error adding column: {err}")
        else:
            print("Column 'duplicate' already exists.")

    def get_sequences(self, table_name):
        select_query = f"""
            SELECT id, qseq_path, identity, alignment_length, query_length
            FROM {table_name}
            WHERE cutoff = 1
        """
        self.cursor.execute(select_query)
        sequences = self.cursor.fetchall()
        return sequences

    def blast_sequences(self, seq1_path, seq2_path):
        # Check if sequence files exist and have content
        if not os.path.isfile(seq1_path) or not os.path.isfile(seq2_path):
            print(f"Sequence file missing: {seq1_path} or {seq2_path}")
            return []

        if os.path.getsize(seq1_path) == 0 or os.path.getsize(seq2_path) == 0:
            print(f"Sequence file is empty: {seq1_path} or {seq2_path}")
            return []

        # Construct and print the BLAST command for debugging
        blast_command = ["blastn", "-query", seq1_path, "-subject", seq2_path, "-outfmt",
                         "10 pident qlen slen qstart qend sstart send"]
        print(f"Running BLAST command: {' '.join(blast_command)}")

        result = subprocess.run(blast_command, capture_output=True, text=True)

        if result.stderr:
            print(f"BLAST error: {result.stderr}")

        blast_output = result.stdout.strip().split(',')
        return blast_output

    def calculate_coverage(self, alignment_length, query_length):
        return (alignment_length / query_length) * 100

    def update_duplicate_column(self, table_name):
        sequences = self.get_sequences(table_name)
        grouped_sequences = {}

        for seq in sequences:
            id, qseq_path, identity, alignment_length, query_length = seq
            coverage = self.calculate_coverage(alignment_length, query_length)
            key = (identity, coverage)
            if key not in grouped_sequences:
                grouped_sequences[key] = []
            permission = 1
            grouped_sequences[key].append((id, qseq_path, permission))

        for key, seq_group in grouped_sequences.items():
            seq_count = len(seq_group)
            for i in range(seq_count):
                if seq_group[i][2] == 1:
                    value = 1
                    for j in range(i + 1, seq_count):
                        id1, seq1_path, permission1 = seq_group[i]
                        id2, seq2_path, permission2 = seq_group[j]
                        blast_output = self.blast_sequences(seq1_path, seq2_path)

                        # Ensure the BLAST output is valid
                        if len(blast_output) < 7:
                            print(f"Invalid BLAST output for {seq1_path} vs {seq2_path}: {blast_output}")
                            continue

                        try:
                            identity = float(blast_output[0])
                            qlen = int(blast_output[1])
                            slen = int(blast_output[2])
                            qstart = int(blast_output[3])
                            qend = int(blast_output[4])
                            sstart = int(blast_output[5])
                            send = int(blast_output[6])
                        except ValueError as e:
                            print(f"Error converting BLAST output: {e}")
                            continue

                        q_coverage = ((qend - qstart + 1) / qlen) * 100
                        s_coverage = ((send - sstart + 1) / slen) * 100


                        if identity < 100:
                            update_query = f"UPDATE {table_name} SET duplicate = 1 WHERE id = {id1}"
                        else:
                            if q_coverage >= s_coverage:
                                update_query = (f"UPDATE {table_name} SET duplicate = 1 WHERE id = {id2}")
                                self.cursor.execute(update_query)
                                update_query = (f"UPDATE {table_name} SET duplicate = 0 WHERE id = {id1}")
                                self.cursor.execute(update_query)
                                value = 0
                            else:
                                update_query = (f"UPDATE {table_name} SET duplicate = 1 WHERE id = {id1};")
                                self.cursor.execute(update_query)
                                update_query = (f"UPDATE {table_name} SET duplicate = 0 WHERE id = {id2}")
                                self.cursor.execute(update_query)
                                seq_group[j][2] = 0
                        self.cursor.execute(update_query)
                        self.mydb.commit()
                        if value == 0:
                            break

    def process_duplicates(self):
        self.connect()
        self.add_duplicate_column(self.gene)
        self.update_duplicate_column(self.gene)
        self.show_database_contents(self.gene)
        self.disconnect()

    def show_database_contents(self, table_name):
        # Query to select all rows from the specified table
        select_query = f"SELECT * FROM {table_name}"

        # Execute the query
        self.cursor.execute(select_query)

        # Fetch all rows from the result set
        rows = self.cursor.fetchall()

        # Print column headers
        print("Database Contents:")
        print("-------------------")
        columns = [desc[0] for desc in self.cursor.description]
        print("\t".join(columns))

        # Print each row
        for row in rows:
            print("\t".join(str(col) for col in row))

