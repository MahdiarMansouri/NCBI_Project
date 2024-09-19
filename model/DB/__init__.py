import os
import mysql.connector
from mysql.connector import Error


# Function to get file details
def get_files(folder_path):
    file_details = []
    for root, dirs, files in os.walk(folder_path):
        for file in files:
            name = file.split('.')[0]
            name = name.replace(" ", "")
            file_path = os.path.join(root, file)
            file_details.append((name, file_path, file))
    return file_details


# Function to connect to MySQL and create table
def create_table_and_insert_data(folder_paths):
    try:
        # Connect to MySQL database
        connection = mysql.connector.connect(
            host='localhost',
            user='root',
            password='mrnd181375',
        )

        if connection.is_connected():
            cursor = connection.cursor()
            cursor.execute("CREATE DATABASE IF NOT EXISTS WGS")
            cursor.execute("USE WGS")

            # Create table query
            # todo: change file_name to gene_name in position 2 instead of column 3

            create_genes_table_query = '''
            CREATE TABLE IF NOT EXISTS gene_files (
                id INT AUTO_INCREMENT PRIMARY KEY,
                name NVARCHAR(255) NOT NULL,
                file_path NVARCHAR(255) NOT NULL,
                file_name NVARCHAR(255) NOT NULL,
                UNIQUE(file_path(255))
            )
            '''

            create_genomes_table_query = '''
                        CREATE TABLE IF NOT EXISTS genome_files (
                            id INT AUTO_INCREMENT PRIMARY KEY,
                            name NVARCHAR(255) NOT NULL,
                            file_path NVARCHAR(255) NOT NULL,
                            file_name NVARCHAR(255) NOT NULL,
                            UNIQUE(file_path(255))
                        )
                        '''
            cursor.execute(create_genes_table_query)
            cursor.execute(create_genomes_table_query)
            connection.commit()

            for idx, name in enumerate(["gene_files", "genome_files"]):
                # Get existing file paths from database
                cursor.execute(f"SELECT file_path FROM {name}")
                existing_files = set(row[0] for row in cursor.fetchall())

                # Get current file details
                current_files = get_files(folder_paths[idx])

                # Insert new files into the table
                new_files = [(name, file_path, file_name) for name, file_path, file_name in current_files if
                             file_path not in existing_files]
                if new_files:
                    insert_query = f"INSERT INTO {name} (name, file_path, file_name) VALUES (%s, %s, %s)"
                    cursor.executemany(insert_query, new_files)
                    connection.commit()

                # Remove paths from database if file no longer exists
                current_file_paths = set(file_path for name, file_path, file_name in current_files)
                files_to_remove = [file_path for file_path in existing_files if file_path not in current_file_paths]
                if files_to_remove:
                    delete_query = f"DELETE FROM {name} WHERE file_path = %s"
                    cursor.executemany(delete_query, [(file_path,) for file_path in files_to_remove])
                    connection.commit()

            print("Database updated successfully.")

    except Error as e:
        print(f"Error: {e}")
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()
            print("MySQL connection is closed.")


# Main function
gene_sample_path = r'C:\Users\mrnaj\OneDrive\Desktop\genes sample'
genome_sample_path = r'C:\Users\mrnaj\OneDrive\Desktop\whole_genome'
create_table_and_insert_data([gene_sample_path, genome_sample_path])
