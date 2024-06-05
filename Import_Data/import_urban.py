"""
This script reads urbanization data from a text file, processes it, and inserts it into a PostgreSQL database. 
It includes functions to read and process the text file, insert the data into the database, and verify the insertion.
"""

import pandas as pd
import psycopg2
import configparser

def main():
    # Read the configuration file
    config = configparser.ConfigParser()
    config.read('config.ini')

    # Get database connection parameters
    db_params = {key: (int(value) if key == 'port' else value) for key, value in config['postgresql'].items()}
    
    file_path = 'Data_Files/urban_data.txt' 

    data = read_and_process_file(file_path)
    insert_data_to_db(data, db_params)

def read_and_process_file(file_path):
    """Read the data from the txt file and save it in a pandas DataFrame."""

    df = pd.read_csv(file_path, delimiter='\t')

    # Rename columns to have consistent names
    df.columns = ['State', '2000', '2010']

    df['2000'] = df['2000'].str.replace(',', '.').astype(float)
    df['2010'] = df['2010'].str.replace(',', '.').astype(float)

    return df

def insert_data_to_db(data, db_params):
    """Insert the processed data into the PostgreSQL database."""

    try:
        with psycopg2.connect(**db_params) as conn:
            with conn.cursor() as cur:
                cur.execute("DROP TABLE IF EXISTS urbanization_data")
                cur.execute("""
                    CREATE TABLE urbanization_data (
                        id SERIAL PRIMARY KEY,
                        state VARCHAR(255),
                        urban_2000 FLOAT,
                        urban_2010 FLOAT
                    )
                """)

                for _, row in data.iterrows():  
                    urban_2000 = round(row['2000']/100, 6)
                    urban_2010 =  round(row['2010']/100, 6)

                    cur.execute("""
                        INSERT INTO urbanization_data (
                            state, urban_2000, urban_2010
                        ) VALUES (%s, %s, %s)
                        ON CONFLICT DO NOTHING
                    """, (row['State'], urban_2000, urban_2010))
                
                conn.commit()
                verify_insertion(cur)

    except Exception as error:
        print(f"Error: {error}")

def verify_insertion(cursor):
    """Query the data to verify insertion."""
    
    cursor.execute("SELECT * FROM urbanization_data")
    rows = cursor.fetchall()
    for row in rows:
        print(row)

if __name__ == '__main__':
    main()