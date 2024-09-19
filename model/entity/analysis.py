import mysql.connector
import pandas as pd


class Analysis:
    def __init__(self, db_info):
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
        except mysql.connector.Error as err:
            print(f"Error: {err}")

    def create_analysis_table(self):
        cursor = self.mydb.cursor()
        create_table_query = """
        CREATE TABLE IF NOT EXISTS gene_analysis (
            gene_name VARCHAR(100) PRIMARY KEY,
            gene_presence_count INT,
            gene_presence_percentage FLOAT,
            cutoff_count INT,
            cutoff_percentage FLOAT,
            duplicate_count INT,
            duplicate_percentage FLOAT,
            diversity_count INT,
            diversity_percentage FLOAT,
            distinct_gene_presence_count INT

        )
        """
        cursor.execute(create_table_query)
        self.mydb.commit()
        cursor.close()
        print("Analysis table created.")

    def analyze_genes(self):
        cursor = self.mydb.cursor()

        # Get all gene tables excluding specific tables
        cursor.execute("SHOW TABLES")
        tables = cursor.fetchall()

        for table in tables:
            table_name = table[0]
            if table_name in ["gene_files", "genome_files", "gene_analysis", "genome_gene"]:
                continue
            # genome_name = cursor.fetchone(f"FROM COLUMNS LIKE 'genome_name'")
            # Total number of entries in the gene table
            # total_query = f"SELECT COUNT(DISTINCT name) FROM genome_files "
            total_blast_query = f"SELECT COUNT(*) FROM {table_name}"
            cursor.execute(total_blast_query)
            total_blast_query_count = cursor.fetchone()[0]
            total_query = f"SELECT COUNT(DISTINCT name) FROM genome_files "
            cursor.execute(total_query)
            total_count = cursor.fetchone()[0]

            # Check for the existence of columns
            cursor.execute(f"SHOW COLUMNS FROM {table_name} LIKE 'cutoff'")
            has_cutoff = cursor.fetchone() is not None

            cursor.execute(f"SHOW COLUMNS FROM {table_name} LIKE 'duplicate'")
            has_duplicate = cursor.fetchone() is not None

            cutoff_count = 0
            cutoff_percentage = 0
            gene_presence_count = 0
            gene_presence_percentage = 0
            distinct_gene_presence_count = 0

            if has_cutoff:
                cutoff_query = f"SELECT COUNT(*) FROM {table_name} WHERE cutoff = 1"
                cursor.execute(cutoff_query)
                gene_presence_count = cursor.fetchone()[0]

                distinct_gene_presence_query = f"SELECT COUNT(DISTINCT genome_name) FROM {table_name} WHERE cutoff = 1"
                cursor.execute(distinct_gene_presence_query)
                distinct_gene_presence_count = cursor.fetchone()[0]

                cutoff_count = (total_blast_query_count - gene_presence_count)
                if total_blast_query_count == 0:
                    continue
                else:
                    cutoff_percentage = (cutoff_count / total_blast_query_count) * 100 if total_count else 0
                    gene_presence_percentage = (distinct_gene_presence_count / total_count) * 100 if total_count else 0

            duplicate_count = 0
            duplicate_percentage = 0
            diversity_count = 0
            diversity_percentage = 0
            if has_duplicate:
                duplicate_query = f"SELECT COUNT(*) FROM {table_name} WHERE duplicate = 1"
                cursor.execute(duplicate_query)
                diversity_count = cursor.fetchone()[0]
                duplicate_count = (gene_presence_count - diversity_count)
                if gene_presence_count == 0:
                    continue
                else:
                    duplicate_percentage = (duplicate_count / gene_presence_count) * 100 if total_count else 0
                    diversity_percentage = (diversity_count / gene_presence_count) * 100 if total_count else 0

            # Insert or update analysis results in gene_analysis table
            insert_query = """
            INSERT INTO gene_analysis (gene_name, cutoff_count, cutoff_percentage, duplicate_count, duplicate_percentage, gene_presence_count, gene_presence_percentage, diversity_count, diversity_percentage, distinct_gene_presence_count)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
            cutoff_count = VALUES(cutoff_count),
            cutoff_percentage = VALUES(cutoff_percentage),
            duplicate_count = VALUES(duplicate_count),
            duplicate_percentage = VALUES(duplicate_percentage),
            gene_presence_count = VALUES(gene_presence_count),
            gene_presence_percentage = VALUES(gene_presence_percentage),
            diversity_count = VALUES(diversity_count),
            diversity_percentage = VALUES(diversity_percentage),
            distinct_gene_presence_count = VALUES(distinct_gene_presence_count)
            """
            cursor.execute(insert_query, (
            table_name, cutoff_count, cutoff_percentage, duplicate_count, duplicate_percentage, gene_presence_count,
            gene_presence_percentage, diversity_count, diversity_percentage, distinct_gene_presence_count))
            self.mydb.commit()

        cursor.close()
        print("Analysis complete.")

    def export_to_excel(self, table_names, output_files):
        cursor = self.mydb.cursor()

        for table_name, output_file in zip(table_names, output_files):
            query = f"SELECT * FROM {table_name}"
            cursor.execute(query)
            rows = cursor.fetchall()

            # Column names
            columns = [desc[0] for desc in cursor.description]

            # Create DataFrame
            df = pd.DataFrame(rows, columns=columns)

            # Export to Excel
            df.to_excel(output_file, index=False)
            print(f"{table_name} exported to {output_file}.")

        cursor.close()

    def create_genome_gene_table(self):
        cursor = self.mydb.cursor()

        # Create the genome_gene table if it doesn't exist
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS genome_gene (
            genome_name VARCHAR(100) PRIMARY KEY
        )
        """)

        # Get all gene tables excluding specific tables
        cursor.execute("SHOW TABLES")
        tables = cursor.fetchall()

        for table in tables:
            gene_name = table[0]
            if gene_name in ["gene_files", "genome_files", "gene_analysis", "genome_gene"]:
                continue

            # Add a column for the gene if it doesn't exist
            cursor.execute(f"SHOW COLUMNS FROM genome_gene LIKE '{gene_name}'")
            if not cursor.fetchone():
                cursor.execute(f"ALTER TABLE genome_gene ADD COLUMN {gene_name} INT DEFAULT 0")

            # Insert genome names into the genome_gene table if they don't exist
            insert_genome_query = """
            INSERT INTO genome_gene (genome_name)
            SELECT DISTINCT name FROM genome_files
            ON DUPLICATE KEY UPDATE genome_name = VALUES(genome_name)
            """
            cursor.execute(insert_genome_query)

            # Update the genome_gene table for genomes that have the gene (cutoff = 1)
            update_query = f"""
            UPDATE genome_gene gg
            JOIN {gene_name} g ON gg.genome_name = REPLACE(g.genome_name, '.fas', '')
            SET gg.{gene_name} = 1
            WHERE g.cutoff = 1
            """
            cursor.execute(update_query)

        self.mydb.commit()
        cursor.close()
        print("Genome-gene association table created.")

    def process_analysis(self, output_files):
        self.connect()
        self.create_analysis_table()
        self.analyze_genes()
        self.create_genome_gene_table()
        self.export_to_excel(['gene_analysis', 'genome_gene'], output_files)
        self.close_connection()

    def close_connection(self):
        if self.mydb:
            self.mydb.close()