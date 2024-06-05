"""
This script reads education data from an Excel file, processes it, and inserts it into a PostgreSQL database. 
It includes functions to read and process the Excel file, insert the data into the database, and verify the insertion.
"""

import pandas as pd
import psycopg2
import configparser
import openpyxl

def main():
    # Read the configuration file
    config = configparser.ConfigParser()
    config.read('config.ini')

    # Get database connection parameters
    db_params = {key: (int(value) if key == 'port' else value) for key, value in config['postgresql'].items()}
    
    file_path = 'Data_Files/education_data.xlsx' 

    data = read_and_process_file(file_path)
    insert_data_to_db(data, db_params)

def read_and_process_file(file_path):
    """Read and process the Excel file, returning a pandas DataFrame."""

    # Load the Excel workbook
    workbook = openpyxl.load_workbook(file_path)

    # Select the active sheet
    sheet = workbook.active

    # Extract data from the sheet into a DataFrame
    data = []
    for row in sheet.iter_rows(min_row=3, values_only=True):  # Assuming the first row is the header
        state = row[0]
        if state is None:
            continue
        
        # Extract data for the years 2000, 2008-2012 (treated as 2008), 2017-2021 (treated as 2017)
        total_2000, urban_2000, rural_2000 = row[4], row[10], row[18]
        total_2008, urban_2008, rural_2008 = row[5], row[11], row[19]
        total_2017, urban_2017, rural_2017 = row[6], row[12], row[20]

        data.append([state, total_2000, total_2008, total_2017, 
                     urban_2000, urban_2008, urban_2017, 
                     rural_2000, rural_2008, rural_2017])

    # Create a DataFrame
    columns = [
        'state', 'total_college_finishers_2000', 'total_college_finishers_2008', 'total_college_finishers_2017',
        'urban_college_finishers_2000', 'urban_college_finishers_2008', 'urban_college_finishers_2017',
        'rural_college_finishers_2000', 'rural_college_finishers_2008', 'rural_college_finishers_2017'
    ]
    df = pd.DataFrame(data, columns=columns)

    return df

def insert_data_to_db(data, db_params):
    """Insert the processed data into the PostgreSQL database."""

    try:
        with psycopg2.connect(**db_params) as conn:
            with conn.cursor() as cur:
                cur.execute("DROP TABLE IF EXISTS education_data")
                cur.execute("""
                    CREATE TABLE education_data (
                        id SERIAL PRIMARY KEY,
                        state VARCHAR(255),
                        total_college_finishers_2000 FLOAT,
                        total_college_finishers_2008 FLOAT,
                        total_college_finishers_2017 FLOAT,
                        urban_college_finishers_2000 FLOAT,
                        urban_college_finishers_2008 FLOAT,
                        urban_college_finishers_2017 FLOAT,
                        rural_college_finishers_2000 FLOAT,
                        rural_college_finishers_2008 FLOAT,
                        rural_college_finishers_2017 FLOAT
                    )
                """)

                for _, row in data.iterrows():
                    cur.execute("""
                        INSERT INTO education_data (
                            state, total_college_finishers_2000, total_college_finishers_2008, total_college_finishers_2017,
                            urban_college_finishers_2000, urban_college_finishers_2008, urban_college_finishers_2017,
                            rural_college_finishers_2000, rural_college_finishers_2008, rural_college_finishers_2017
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT DO NOTHING
                    """, (row['state'], row['total_college_finishers_2000'], row['total_college_finishers_2008'], row['total_college_finishers_2017'], 
                          row['urban_college_finishers_2000'], row['urban_college_finishers_2008'], row['urban_college_finishers_2017'], 
                          row['rural_college_finishers_2000'], row['rural_college_finishers_2008'], row['rural_college_finishers_2017']))
                
                conn.commit()
                verify_insertion(cur)

    except Exception as error:
        print(f"Error: {error}")

def verify_insertion(cursor):
    """Query the data to verify insertion."""
    
    cursor.execute("SELECT * FROM education_data")
    rows = cursor.fetchall()
    for row in rows:
        print(row)

if __name__ == '__main__':
    main()