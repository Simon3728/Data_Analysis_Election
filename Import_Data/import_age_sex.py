""" 
This script reads age and sex demographic data from a CSV file, processes it, and inserts it into a PostgreSQL database. 
It includes functions to read and process the CSV file, insert the data into the database, and verify the insertion.
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
    
    file_path = 'Data_Files/age_sex_data.csv' 

    data = read_and_process_file(file_path)
    insert_data_to_db(data, db_params)

def read_and_process_file(file_path):
    """Read and process the CSV file, returning a pandas DataFrame."""
    
    # Load the CSV file
    data = pd.read_csv(file_path)

    # Filter out county data
    state_data = data[(data['Countyfips'] == 0) & (data['Statefips'] != 0)]

    return state_data

def insert_data_to_db(data, db_params):
    """Insert the processed data into the PostgreSQL database."""
    try:
        with psycopg2.connect(**db_params) as conn:
            with conn.cursor() as cur:
                cur.execute("DROP TABLE IF EXISTS population_data")
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS population_data (
                        id SERIAL PRIMARY KEY,
                        name VARCHAR(255),
                        year INTEGER,
                        total_population INTEGER,
                        population_0_4 FLOAT,
                        population_5_17 FLOAT,
                        population_18_24 FLOAT,
                        population_25_44 FLOAT,
                        population_45_64 FLOAT,
                        population_65_plus FLOAT,
                        population_under_18 FLOAT,
                        population_18_54 FLOAT,
                        population_55_plus FLOAT,
                        male_population FLOAT,
                        female_population FLOAT
                    )
                """)

                for _, row in data.iterrows():
                    cur.execute("""
                        INSERT INTO population_data (
                            name, year, total_population, population_0_4, population_5_17, 
                            population_18_24, population_25_44, population_45_64, 
                            population_65_plus, population_under_18, population_18_54, 
                            population_55_plus, male_population, female_population
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT DO NOTHING
                    """, (
                        row['Description'], row['Year'], row['Total Population'], row['Population 0-4'],
                        row['Population 5-17'], row['Population 18-24'], row['Population 25-44'],
                        row['Population 45-64'], row['Population 65+'], row['Population Under 18'],
                        row['Population 18-54'], row['Population 55+'], row['Male Population'],
                        row['Female Population']
                    ))

                conn.commit()
                verify_insertion(cur)

    except Exception as error:
        print(f"Error: {error}")

def verify_insertion(cursor):
    """Query the data to verify insertion."""
    
    cursor.execute("SELECT * FROM population_data")
    rows = cursor.fetchall()
    for row in rows:
        print(row)

if __name__ == '__main__':
    main()
