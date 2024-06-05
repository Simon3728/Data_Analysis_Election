"""
This script reads unemployment data from a CSV file, processes it, and inserts it into a PostgreSQL database. 
It includes functions to read and process the CSV file, insert the data into the database, and verify the insertion.
"""

import pandas as pd
import psycopg2
import configparser

# Mapping from state abbreviations to state names
state_abbr_to_name = {
    'AKURN': 'Alaska', 'ALURN': 'Alabama', 'ARURN': 'Arkansas', 'AZURN': 'Arizona', 'CAURN': 'California',
    'COURN': 'Colorado', 'CTURN': 'Connecticut', 'DCURN': 'District of Columbia', 'DEURN': 'Delaware',
    'FLURN': 'Florida', 'GAURN': 'Georgia', 'HIURN': 'Hawaii', 'IAURN': 'Iowa', 'IDURN': 'Idaho',
    'ILURN': 'Illinois', 'INURN': 'Indiana', 'KSURN': 'Kansas', 'KYURN': 'Kentucky', 'LAURN': 'Louisiana',
    'MAURN': 'Massachusetts', 'MDURN': 'Maryland', 'MEURN': 'Maine', 'MIURN': 'Michigan', 'MNURN': 'Minnesota',
    'MOURN': 'Missouri', 'MSURN': 'Mississippi', 'MTURN': 'Montana', 'NCURN': 'North Carolina', 'NDURN': 'North Dakota',
    'NEURN': 'Nebraska', 'NHURN': 'New Hampshire', 'NJURN': 'New Jersey', 'NMURN': 'New Mexico', 'NVURN': 'Nevada',
    'NYURN': 'New York', 'OHURN': 'Ohio', 'OKURN': 'Oklahoma', 'ORURN': 'Oregon', 'PAURN': 'Pennsylvania',
    'PRURN': 'Puerto Rico', 'RIURN': 'Rhode Island', 'SCURN': 'South Carolina', 'SDURN': 'South Dakota',
    'TNURN': 'Tennessee', 'TXURN': 'Texas', 'UTURN': 'Utah', 'VAURN': 'Virginia', 'VTURN': 'Vermont',
    'WAURN': 'Washington', 'WIURN': 'Wisconsin', 'WVURN': 'West Virginia', 'WYURN': 'Wyoming'
}

def main():
    # Read the configuration file
    config = configparser.ConfigParser()
    config.read('config.ini')

    # Get database connection parameters
    db_params = {key: (int(value) if key == 'port' else value) for key, value in config['postgresql'].items()}

    file_path = 'Data_Files/unemployment_data.csv'

    data = read_and_process_file(file_path)
    insert_data_to_db(data, db_params)

def read_and_process_file(file_path):
    """Read and process the CSV file, returning a pandas DataFrame."""

    # Read the file and manually split columns
    with open(file_path, 'r') as file:
        lines = file.readlines()
    
    # Manually split the columns by tab and strip extra spaces and quotes
    columns = lines[0].strip().split('\t')
    columns = [col.strip('"') for col in columns]  # Remove any leading/trailing quotes
    data_lines = [line.strip().split('\t') for line in lines[1:]]

    data = pd.DataFrame(data_lines, columns=columns)
    
    # Ensure the DATE column is properly cleaned
    data['DATE'] = data['DATE'].str.replace('"', '').str.strip()
    data['WYURN'] = data['WYURN'].str.replace('"', '').str.strip()
    
    # Extract the year directly from the DATE column
    data['YEAR'] = data['DATE'].str[:4].apply(lambda x: int(''.join(filter(str.isdigit, x))))

    # Drop the old DATE column
    data = data.drop(columns=['DATE'])

    for col in data.columns:
        data[col] = pd.to_numeric(data[col], errors='coerce')
    
    # Calculate the average yearly unemployment for each state
    yearly_avg = data.groupby('YEAR').mean().reset_index()    

    # Replace state abbreviations with full state names
    yearly_avg.rename(columns=state_abbr_to_name, inplace=True)
    
    # Melt the DataFrame to have a format suitable for the database
    melted = yearly_avg.melt(id_vars=['YEAR'], var_name='state', value_name='avg_unemployment')

    return melted

def insert_data_to_db(data, db_params):
    """Insert the processed data into the PostgreSQL database."""

    try:
        with psycopg2.connect(**db_params) as conn:
            with conn.cursor() as cur:
                cur.execute("DROP TABLE IF EXISTS unemployment_data")
                cur.execute("""
                    CREATE TABLE unemployment_data (
                        id SERIAL PRIMARY KEY,
                        state VARCHAR(255),
                        year INT,
                        unemployment FLOAT
                    )
                """)

                for _, row in data.iterrows():
                    cur.execute("""
                        INSERT INTO unemployment_data (state, year, unemployment)
                        VALUES (%s, %s, %s)
                        ON CONFLICT DO NOTHING
                    """, (row['state'], row['YEAR'], round(row['avg_unemployment']/100, 4)))

                conn.commit()
                verify_insertion(cur)

    except Exception as error:
        print(f"Error: {error}")

def verify_insertion(cursor):
    """Query the data to verify insertion."""
    
    cursor.execute("SELECT * FROM unemployment_data")
    rows = cursor.fetchall()
    for row in rows:
        print(row)

if __name__ == '__main__':
    main()
