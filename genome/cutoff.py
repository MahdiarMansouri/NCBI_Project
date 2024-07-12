import mysql.connector

class UpdateDB:
    def __init__(self, gene, db_info):
        self.gene = gene
        self.db_info = db_info
        self.mydb = None

    def connect(self):
        try:
            self.mydb = mysql.connector.connect(
                host=self.db_info['host'],
                user=self.db_info['user'],
                passwd=self.db_info['passwd'],
                database=self.db_info['database']
            )
            print("Successfully connected to the database.")
        except mysql.connector.Error as err:
            print(f"Error: {err}")

    def add_cutoff_column(self, table_name):
        cursor = self.mydb.cursor()
        alter_table_query = f"ALTER TABLE {table_name} ADD COLUMN cutoff TINYINT"
        try:
            cursor.execute(alter_table_query)
            self.mydb.commit()
            print(f"Column 'cutoff' added to {table_name} table.")
        except mysql.connector.Error as err:
            print(f"Error adding column: {err}")
        finally:
            cursor.close()

    def update_cutoff_column(self, table_name):
        cursor = self.mydb.cursor()
        update_query = f"""
            UPDATE {table_name}
            SET cutoff = CASE
                WHEN identity < 85 OR (alignment_length / query_length) * 100 < 90 THEN 0
                ELSE 1
            END
        """
        try:
            cursor.execute(update_query)
            self.mydb.commit()
            print(f"Column 'cutoff' updated in {table_name} table.")
        except mysql.connector.Error as err:
            print(f"Error updating column: {err}")
        finally:
            cursor.close()

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
        columns = [desc[0] for desc in cursor.description]
        print("\t".join(columns))

        # Print each row
        for row in rows:
            print("\t".join(str(col) for col in row))

        cursor.close()

    def close_connection(self):
        if self.mydb:
            self.mydb.close()


# Information input
db_info = {
    'host': 'localhost',
    'user': 'root',
    'passwd': 'mrnd181375',
    'database': 'wgs'
}
Gene = "mepa"

# Run functions for updating the database
updater = UpdateDB(Gene, db_info)
updater.connect()
updater.add_cutoff_column(Gene)
updater.update_cutoff_column(Gene)
updater.show_database_contents(Gene)
updater.close_connection()
