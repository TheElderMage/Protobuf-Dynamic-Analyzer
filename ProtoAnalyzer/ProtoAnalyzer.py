import gzip
import sqlite3
import zlib
import os
import blackboxprotobuf

def open_sqlite_db_readonly(path):
    '''Opens an SQLite database in read-only mode.'''
    if not os.path.isfile(path):
        raise FileNotFoundError(f"The database file '{path}' does not exist.")
    
    uri_path = f'file:{path}?mode=ro'
    conn = sqlite3.connect(uri_path, uri=True)
    return conn

def handle_protobuf_file(file_path):
    '''Handles direct protobuf file decoding.'''
    try:
        # Read the binary data from the protobuf file
        with open(file_path, 'rb') as f:
            binary_data = f.read()

        # Decode the message using blackboxprotobuf
        message, typedef = blackboxprotobuf.decode_message(binary_data)
        return [message]  # Returning a list to keep it consistent with database result
    except (blackboxprotobuf.DecodingError, KeyError, TypeError, ValueError) as e:
        print(f"Error decoding protobuf file: {e}")
        return []

def handle_sqlite_db(db_path):
    '''Extracts binary data from the SQLite database and decodes it using blackboxprotobuf.'''
    try:
        # Open the SQLite database in read-only mode
        db = open_sqlite_db_readonly(db_path)
        cursor = db.cursor()

        # Query to select all binary data from the messages_v2 table
        cursor.execute('') #   <<<<<<<<-------------------------------------- query if data is in SQLITE

        all_rows = cursor.fetchall()
        decoded_data = []

        for row in all_rows:
            binary_data = row[0]  # The binary data is in the first column

            if binary_data:
                try:
                    # Decompress and decode the data
                    data_array = bytearray(binary_data)
                    


                    # data_array = data_array[0:]   <<<<<<<--------------------------------------- # Remove bytes if needed
                    


                    # decompressed_data = zlib.decompress(data_array)  <<<<<<<<<----------------------------- # decompress if needed
                    



                    # Dynamically decode the message using blackboxprotobuf
                    message, typedef = blackboxprotobuf.decode_message(data_array)

                    # Append decoded message
                    decoded_data.append(message)

                except (zlib.error, KeyError, TypeError, ValueError) as e:
                    # Log the error and continue processing the next row
                    print(f"Error processing a record: {e}")
                    continue

        db.close()
        return decoded_data

    except sqlite3.Error as e:
        print(f"An error occurred while accessing the database: {e}")
        return []

def extract_binary_data(db_path, output_file_path):
    '''Determines if the input file is a protobuf file or SQLite database and decodes it.'''
    decoded_data = []

    # Check if the file is an SQLite database or a raw protobuf file
    if db_path.endswith('.db'):  # If it's an SQLite database
        print("Processing as SQLite database...")
        decoded_data = handle_sqlite_db(db_path)
    else:  # Otherwise, assume it's a raw protobuf file
        print("Processing as raw protobuf file...")
        decoded_data = handle_protobuf_file(db_path)

    # Write the decoded data to the output file
    with open(output_file_path, 'w', encoding='utf-8') as file:
        for message in decoded_data:
            file.write(f"{message}\n")
            file.write("-" * 50 + "\n")

    print(f"Decoded data has been saved to {output_file_path}.")

# Replace with your actual database path and output file path
db_path = r"" #  <<<<<<<<--------------------------------  File Path


output_file_path = r""  #  <<<<<<<<------------------------------------------------------  OutPut Directory Path

# Call the function to extract and decode the data
extract_binary_data(db_path, output_file_path)


