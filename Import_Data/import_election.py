"""
This script reads election data from a CSV file, processes it, and inserts it into a PostgreSQL database. 
It includes functions to read and process the CSV file, insert the data into the database, and verify the insertion.
"""

import pandas as pd
import psycopg2
import configparser
import re

# Dictanary for mapping to state names
state_names = {
    'AL': 'Alabama', 'AK': 'Alaska', 'AZ': 'Arizona', 'AR': 'Arkansas', 'CA': 'California',
    'CO': 'Colorado', 'CT': 'Connecticut', 'DC': 'District of Columbia', 'DE': 'Delaware',
    'FL': 'Florida', 'GA': 'Georgia', 'HI': 'Hawaii', 'ID': 'Idaho', 'IL': 'Illinois', 'IN': 'Indiana',
    'IA': 'Iowa', 'KS': 'Kansas', 'KY': 'Kentucky', 'LA': 'Louisiana', 'ME': 'Maine', 'MD': 'Maryland',
    'MA': 'Massachusetts', 'MI': 'Michigan', 'MN': 'Minnesota', 'MS': 'Mississippi', 'MO': 'Missouri',
    'MT': 'Montana', 'NE': 'Nebraska', 'NV': 'Nevada', 'NH': 'New Hampshire', 'NJ': 'New Jersey',
    'NM': 'New Mexico', 'NY': 'New York', 'NC': 'North Carolina', 'ND': 'North Dakota', 'OH': 'Ohio',
    'OK': 'Oklahoma', 'OR': 'Oregon', 'PA': 'Pennsylvania', 'RI': 'Rhode Island', 'SC': 'South Carolina',
    'SD': 'South Dakota', 'TN': 'Tennessee', 'TX': 'Texas', 'UT': 'Utah', 'VT': 'Vermont',
    'VA': 'Virginia', 'WA': 'Washington', 'WV': 'West Virginia', 'WI': 'Wisconsin', 'WY': 'Wyoming'
}

def main():
    # Read the configuration file
    config = configparser.ConfigParser()
    config.read('config.ini')

    # Get database connection parameters
    db_params = {key: (int(value) if key == 'port' else value) for key, value in config['postgresql'].items()}

    file_paths = [
        'Data_Files/Election/president_2000.txt', 'Data_Files/Election/president_2004.txt', 
        'Data_Files/Election/president_2008.txt', 'Data_Files/Election/president_2012.txt', 
        'Data_Files/Election/president_2016.txt', 'Data_Files/Election/president_2020.txt'
    ]

    # Process each file and combine the data
    all_data = pd.DataFrame()
    for file_path in file_paths:
        data_percentage = read_and_process_file(file_path)
        all_data = pd.concat([all_data, data_percentage], ignore_index=True)
    
    # Insert all data into the database
    insert_data_to_db(all_data, db_params)


def read_and_process_file(file_path):
    """Read and process the election data file."""

    # Extract year from filename
    year_match = re.search(r'president_(\d{4})\.txt', file_path)
    year = int(year_match.group(1)) if year_match else None

    with open(file_path, 'r') as file:
        lines = file.readlines()

    # Extract the header and data
    header = lines[0].strip().split()
    data = [line.strip().split() for line in lines[1:]]

    # Map the header to expected columns
    column_mapping = {
        'State': 'StateCode',
        'Republican': 'Republican',
        'Democratic': 'Democratic',
        'Others': 'Others',
        'Total': 'Total'
    }

    # Ensure the header columns are in the expected order
    columns = [column_mapping[col] for col in header if col in column_mapping]

    df = pd.DataFrame(data, columns=columns)

    # Clean numeric data by removing commas
    numeric_columns = ['Republican', 'Democratic', 'Others', 'Total']
    for column in numeric_columns:
        df[column] = df[column].str.replace(',', '').astype(float)

    # Add the 'Year' column and map state names
    df['Year'] = year
    df['State'] = df['StateCode'].map(state_names)

    return df[['State', 'Year', 'Republican', 'Democratic', 'Others', 'Total']]

def insert_data_to_db(data, db_params):
    """Insert the processed data into the PostgreSQL database."""

    try:
        with psycopg2.connect(**db_params) as conn:
            with conn.cursor() as cur:
                cur.execute("DROP TABLE IF EXISTS election_results")
                cur.execute("""
                    CREATE TABLE election_results (
                        id SERIAL PRIMARY KEY,
                        state VARCHAR(255),
                        year INTEGER,
                        republican INTEGER,
                        democratic INTEGER,
                        others INTEGER,
                        total INTEGER
                    )
                """)

                for _, row in data.iterrows():
                    cur.execute("""
                        INSERT INTO election_results (state, year, republican, democratic, others, total)
                        VALUES (%s, %s, %s, %s, %s, %s)
                    """, (row['State'], row['Year'], row['Republican'], row['Democratic'], row['Others'], row['Total']))

                conn.commit()
                verify_insertion(cur)

    except Exception as error:
        print(f"Error: {error}")


def verify_insertion(cursor):
    """Query the data to verify insertion."""
    
    cursor.execute("SELECT * FROM election_results")
    rows = cursor.fetchall()
    for row in rows:
        print(row)

if __name__ == '__main__':
    main()
