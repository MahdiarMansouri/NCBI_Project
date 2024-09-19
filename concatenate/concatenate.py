import mysql.connector
import pandas as pd
import os


class Concatenate:
    def __init__(self, db_info, output_dir):
        self.db_info = db_info
        self.output_dir = output_dir
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
        except mysql.connector.Error as err:
            print(f"Error: {err}")

    def fetch_genome_gene_data(self):
        cursor = self.mydb.cursor()
        cursor.execute("SELECT * FROM genome_gene")
        rows = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        cursor.close()
        return pd.DataFrame(rows, columns=columns)

    def fetch_sseq_file(self, gene_name, genome_name):
        cursor = self.mydb.cursor()
        query = f"SELECT sseq_path FROM {gene_name} WHERE genome_name = '{genome_name}.fas' AND cutoff = 1"
        cursor.execute(query)
        result = cursor.fetchone()  # Fetch the first result
        cursor.fetchall()  # Fetch the remaining results to clear the cursor
        cursor.close()

        # Debugging output to check what is being fetched
        if result:
            print(f"Found sseq_path for {genome_name} in {gene_name}: {result[0]}")
        else:
            print(f"No sseq_path found for {genome_name} in {gene_name}")

        return result[0] if result else None

    def concatenate_files(self):
        genome_gene_df = self.fetch_genome_gene_data()

        for _, row in genome_gene_df.iterrows():
            genome_name = row['genome_name']
            output_file_path = os.path.join(self.output_dir, f"{genome_name}.fasta")

            with open(output_file_path, 'w') as output_file:
                for gene_name in genome_gene_df.columns[1:]:
                    if row[gene_name] == 1:
                        sseq_file_path = self.fetch_sseq_file(gene_name, genome_name)
                        if sseq_file_path:
                            # Remove the .fasta extension
                            sseq_file_path = sseq_file_path.replace('.fasta', '')

                            # Debugging: Print the original and modified paths
                            print(f"Original sseq_file_path: {sseq_file_path}")

                            full_sseq_file_path = os.path.abspath(sseq_file_path).replace('concatenate', '')

                            # Debugging: Print the full path being checked
                            print(f"Checking path: {full_sseq_file_path}")

                            # Check if the file exists without .fasta extension
                            if not os.path.exists(full_sseq_file_path):
                                print(f"File not found without .fasta: {full_sseq_file_path}")
                                # If it doesn't exist, try adding .fasta
                                full_sseq_file_path = full_sseq_file_path + '.fasta'

                            # Debugging: Print the path with .fasta extension
                            print(f"Checking path with .fasta: {full_sseq_file_path}")

                            if os.path.exists(full_sseq_file_path):
                                with open(full_sseq_file_path, 'r') as gene_file:
                                    content = (f'\n{gene_name} {gene_file.read()}\n')
                                    print(
                                        f"Reading from {full_sseq_file_path} - First 100 characters:\n{content[:100]}")
                                    output_file.write(content)
                            else:
                                print(f"File not found: {full_sseq_file_path}")

            print(f"Concatenated file created for genome: {genome_name}")

    def process_concatenation(self):
        self.connect()
        self.concatenate_files()
        self.close_connection()

    def close_connection(self):
        if self.mydb:
            self.mydb.close()


# Database information:
db_info = {
    'host': 'localhost',
    'user': 'root',
    'password': 'mrnd181375',
    'database': 'wgs'
}

output_dir = 'C://Users//mrnaj//PycharmProjects//NCBI_project_2//concatenate//result'

concatenate = Concatenate(db_info, output_dir)
concatenate.process_concatenation()
