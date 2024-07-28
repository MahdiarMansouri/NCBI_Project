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
            diversity_percentage FLOAT
            
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
            if table_name in ["gene_files", "genome_files", "gene_analysis"]:
                continue
            # Total number of entries in the gene table
            total_query = f"SELECT COUNT(DISTINCT {genome_name}) FROM {table_name} "
            cursor.execute(total_query)
            total_count = cursor.fetchone()[0]
            print(f"{table_name} has {total_count} 11111111111111111111111111111111111111111111111111111111111")
            # Check for the existence of columns
            cursor.execute(f"SHOW COLUMNS FROM {table_name} LIKE 'cutoff'")
            has_cutoff = cursor.fetchone() is not None

            cursor.execute(f"SHOW COLUMNS FROM {table_name} LIKE 'duplicate'")
            has_duplicate = cursor.fetchone() is not None

            cutoff_count = 0
            cutoff_percentage = 0
            gene_presence_count = 0
            gene_presence_percentage = 0
            if has_cutoff:
                cutoff_query = f"SELECT COUNT(*) FROM {table_name} WHERE cutoff = 1"
                cursor.execute(cutoff_query)
                gene_presence_count = cursor.fetchone()[0]
                cutoff_count = (total_count - gene_presence_count)
                cutoff_percentage = (cutoff_count / total_count) * 100 if total_count else 0
                gene_presence_percentage = (gene_presence_count / total_count) * 100 if total_count else 0

            duplicate_count = 0
            duplicate_percentage = 0
            diversity_count = 0
            diversity_percentage = 0
            if has_duplicate:
                duplicate_query = f"SELECT COUNT(*) FROM {table_name} WHERE duplicate = 1"
                cursor.execute(duplicate_query)
                diversity_count = cursor.fetchone()[0]
                duplicate_count = (gene_presence_count - diversity_count)
                duplicate_percentage = (duplicate_count / gene_presence_count) * 100 if total_count else 0
                diversity_percentage = (diversity_count / gene_presence_count) * 100 if total_count else 0




            # Insert or update analysis results in gene_analysis table
            insert_query = """
            INSERT INTO gene_analysis (gene_name, cutoff_count, cutoff_percentage, duplicate_count, duplicate_percentage, gene_presence_count, gene_presence_percentage, diversity_count, diversity_percentage)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
            cutoff_count = VALUES(cutoff_count),
            cutoff_percentage = VALUES(cutoff_percentage),
            duplicate_count = VALUES(duplicate_count),
            duplicate_percentage = VALUES(duplicate_percentage),
            gene_presence_count = VALUES(gene_presence_count),
            gene_presence_percentage = VALUES(gene_presence_percentage),
            diversity_count = VALUES(diversity_count),
            diversity_percentage = VALUES(diversity_percentage)
            """
            cursor.execute(insert_query, (table_name, cutoff_count, cutoff_percentage, duplicate_count, duplicate_percentage, gene_presence_count, gene_presence_percentage, diversity_count, diversity_percentage))
            self.mydb.commit()

        cursor.close()
        print("Analysis complete.")

    def export_to_excel(self, output_file):
        cursor = self.mydb.cursor()
        cursor.execute("SELECT * FROM gene_analysis")
        rows = cursor.fetchall()

        # Column names
        columns = [desc[0] for desc in cursor.description]

        # Create DataFrame
        df = pd.DataFrame(rows, columns=columns)

        # Export to Excel
        df.to_excel(output_file, index=False)
        print(f"Analysis results exported to {output_file}.")

        cursor.close()

    def process_analysis(self, output_file):
        self.connect()
        self.create_analysis_table()
        self.analyze_genes()
        self.export_to_excel(output_file)
        self.close_connection()

    def close_connection(self):
        if self.mydb:
            self.mydb.close()
