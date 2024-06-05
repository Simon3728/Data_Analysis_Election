"""
This script reads GDP data for US states from a CSV file, processes it, and inserts it into a PostgreSQL database. 
It includes functions to read and process the CSV file, insert the data into the database, and verify the insertion.
"""


import pandas as pd
import psycopg2
import configparser

# ALl valid states
VALID_STATES = [
    'Alabama', 'Alaska', 'Arizona', 'Arkansas', 'California', 'Colorado', 'Connecticut', 'Delaware', 'Florida',
    'Georgia', 'Hawaii', 'Idaho', 'Illinois', 'Indiana', 'Iowa', 'Kansas', 'Kentucky', 'Louisiana', 'Maine',
    'Maryland', 'Massachusetts', 'Michigan', 'Minnesota', 'Mississippi', 'Missouri', 'Montana', 'Nebraska',
    'Nevada', 'New Hampshire', 'New Jersey', 'New Mexico', 'New York', 'North Carolina', 'North Dakota', 'Ohio',
    'Oklahoma', 'Oregon', 'Pennsylvania', 'Rhode Island', 'South Carolina', 'South Dakota', 'Tennessee', 'Texas',
    'Utah', 'Vermont', 'Virginia', 'Washington', 'West Virginia', 'Wisconsin', 'Wyoming', 'United States'
]


def main():
    # Read the configuration file
    config = configparser.ConfigParser()
    config.read('config.ini')

    # Get database connection parameters
    db_params = {key: (int(value) if key == 'port' else value) for key, value in config['postgresql'].items()}

    file_path = 'Data_files/state_gdp.csv'

    data = read_and_process_file(file_path)
    insert_data_to_db(db_params, data)

def read_and_process_file(file_path):
    """Read the CSV file and process the data for insertion."""

    data = pd.read_csv(file_path)

    filtered_data = data[data['Description'].str.strip() == 'All industry total']
    
    # Melt the dataframe to convert year columns into rows
    melted_data = filtered_data.melt(
        id_vars=['GeoName'], 
        value_vars=[str(year) for year in range(1997, 2023)], 
        var_name='Year', 
        value_name='GDP'
    )

    # Filter the melted data to include only rows where GeoName is in the valid_states list
    filtered_final_data = melted_data[melted_data['GeoName'].isin(VALID_STATES)]

    return filtered_final_data

def insert_data_to_db(db_params, data):
    """Insert the processed data into the PostgreSQL database."""

    try:
        with psycopg2.connect(**db_params) as conn:
            with conn.cursor() as cur:
                cur.execute("DROP TABLE IF EXISTS gdp_data")
                
                cur.execute("""
                    CREATE TABLE gdp_data (
                        id SERIAL PRIMARY KEY,
                        state VARCHAR(255),
                        year INT,
                        total_gpd FLOAT
                    )
                """)
                
                for _, row in data.iterrows():
                    cur.execute("""
                        INSERT INTO gdp_data (
                            state, year, total_gpd
                        ) VALUES (%s, %s, %s)
                        ON CONFLICT DO NOTHING
                    """, (row['GeoName'], int(row['Year']), float(row['GDP'])))
                
                conn.commit()
                verify_insertion(cur)
                
    except Exception as error:
        print(f"Error: {error}")

def verify_insertion(cursor):
    """Query the data to verify insertion."""
    
    cursor.execute("SELECT * FROM gdp_data")
    rows = cursor.fetchall()
    for row in rows:
        print(row)


if __name__ == '__main__':
    main()
