import os
import mysql.connector
from mysql.connector import Error


# Function to get file details
def get_files(folder_path):
    file_details = []
    for root, dirs, files in os.walk(folder_path):
        for file in files:
            file_path = os.path.join(root, file)
            file_details.append((file_path, file))
    return file_details


# Function to create database
def create_database(cursor):
    cursor.execute("CREATE DATABASE IF NOT EXISTS WGS")


# Function to connect to MySQL and create table
def create_table_and_insert_data(folder_path):
    try:
        # Connect to MySQL database
        connection = mysql.connector.connect(
            host='localhost',
            user='root',
            password='root123',
            database='WGS'
        )

        if connection.is_connected():
            cursor = connection.cursor()

            # Create and use the database
            create_database(cursor)

            # Create table query
            create_table_query = '''
            CREATE TABLE IF NOT EXISTS genes_sample_files (
                id INT AUTO_INCREMENT PRIMARY KEY,
                file_path VARCHAR(255) NOT NULL,
                file_name VARCHAR(255) NOT NULL,
                UNIQUE(file_path(255))
            )
            '''
            cursor.execute(create_table_query)
            connection.commit()

            # Get existing file paths from database
            cursor.execute("SELECT file_path FROM genes_sample_files")
            existing_files = set(row[0] for row in cursor.fetchall())

            # Get current file details
            current_files = get_files(folder_path)

            # Insert new files into the table
            new_files = [(file_path, file_name) for file_path, file_name in current_files if
                         file_path not in existing_files]
            if new_files:
                insert_query = "INSERT INTO genes_sample_files (file_path, file_name) VALUES (%s, %s)"
                cursor.executemany(insert_query, new_files)
                connection.commit()

            # Remove paths from database if file no longer exists
            current_file_paths = set(file_path for file_path, file_name in current_files)
            files_to_remove = [file_path for file_path in existing_files if file_path not in current_file_paths]
            if files_to_remove:
                delete_query = "DELETE FROM genes_sample_files WHERE file_path = %s"
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
def main():
    folder_path = r'C:\Users\mrnaj\OneDrive\Desktop\genes sample'
    create_table_and_insert_data(folder_path)


if __name__ == "__main__":
    main()
