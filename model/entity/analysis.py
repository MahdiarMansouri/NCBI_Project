import mysql.connector


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
            cutoff_count INT,
            cutoff_percentage FLOAT,
            duplicate_count INT,
            duplicate_percentage FLOAT
        )
        """
        cursor.execute(create_table_query)
        self.mydb.commit()
        cursor.close()
        print("Analysis table created.")

    def analyze_genes(self):
        cursor = self.mydb.cursor()

        # Get all gene tables
        cursor.execute("SHOW TABLES")
        tables = cursor.fetchall()

        for table in tables:
            table_name = table[0]

            # Total number of entries in the gene table
            total_query = f"SELECT COUNT(*) FROM {table_name}"
            cursor.execute(total_query)
            total_count = cursor.fetchone()[0]

            # Check for the existence of columns
            cursor.execute(f"SHOW COLUMNS FROM {table_name} LIKE 'cutoff'")
            has_cutoff = cursor.fetchone() is not None

            cursor.execute(f"SHOW COLUMNS FROM {table_name} LIKE 'duplicate'")
            has_duplicate = cursor.fetchone() is not None

            cutoff_count = 0
            cutoff_percentage = 0
            if has_cutoff:
                cutoff_query = f"SELECT COUNT(*) FROM {table_name} WHERE cutoff = 1"
                cursor.execute(cutoff_query)
                cutoff_count = cursor.fetchone()[0]
                cutoff_percentage = (cutoff_count / total_count) * 100 if total_count else 0

            duplicate_count = 0
            duplicate_percentage = 0
            if has_duplicate:
                duplicate_query = f"SELECT COUNT(*) FROM {table_name} WHERE duplicate = 1"
                cursor.execute(duplicate_query)
                duplicate_count = cursor.fetchone()[0]
                duplicate_percentage = (duplicate_count / total_count) * 100 if total_count else 0

            # Insert or update analysis results in gene_analysis table
            insert_query = """
            INSERT INTO gene_analysis (gene_name, cutoff_count, cutoff_percentage, duplicate_count, duplicate_percentage)
            VALUES (%s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
            cutoff_count = VALUES(cutoff_count),
            cutoff_percentage = VALUES(cutoff_percentage),
            duplicate_count = VALUES(duplicate_count),
            duplicate_percentage = VALUES(duplicate_percentage)
            """
            cursor.execute(insert_query,
                           (table_name, cutoff_count, cutoff_percentage, duplicate_count, duplicate_percentage))
            self.mydb.commit()

        cursor.close()
        print("Analysis complete.")

    def process_analysis(self):
        self.connect()
        self.create_analysis_table()
        self.analyze_genes()
        self.close_connection()

    def close_connection(self):
        if self.mydb:
            self.mydb.close()
