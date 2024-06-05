"""
This script reads ethnic demographic data from a CSV file, processes it, and inserts it into a PostgreSQL database. 
It includes functions to read and process the CSV file, insert the data into the database, and verify the insertion.
"""

import pandas as pd
import psycopg2
import configparser

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
    
    file_path = 'Data_Files/ethnic_data.csv' 

    data = read_and_process_file(file_path)
    insert_data_to_db(data, db_params)

def read_and_process_file(file_path):
    """Read the CSV file and process the data for insertion."""

    # Load the data from the CSV file
    data = pd.read_csv(file_path)

    # Fill null values with zero before aggregation
    data.fillna(0, inplace=True)

    # Group by State and Year, and sum the numerical columns
    processed_data = data.groupby(['State', 'Year']).sum().reset_index()
    processed_data['State'] = processed_data['State'].map(state_names)

    return processed_data  

def insert_data_to_db(data, db_params):
    """Insert the processed data into the PostgreSQL database."""

    try:
        with psycopg2.connect(**db_params) as conn:
            with conn.cursor() as cur:
                cur.execute("DROP TABLE IF EXISTS ethnic_data")
                cur.execute("""
                    CREATE TABLE ethnic_data (
                        id SERIAL PRIMARY KEY,
                        state VARCHAR(255),
                        year INT,
                        total_population INT,
                        white_alone INT,
                        black_alone INT,
                        american_indian_alone INT,
                        asian_alone INT,
                        pacific_islander_alone INT,
                        two_or_more_races INT,
                        not_hispanic INT,
                        hispanic INT
                    )
                """)
                
                for _, row in data.iterrows():
                    cur.execute("""
                        INSERT INTO ethnic_data (
                            state, year, total_population, white_alone, black_alone,
                            american_indian_alone, asian_alone, pacific_islander_alone,
                            two_or_more_races, not_hispanic, hispanic
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT DO NOTHING
                    """, (
                        row['State'], row['Year'], row['Total Population'], row['White Alone'], row['Black Alone'],
                        row['American Indian or Alaskan Native'], row['Asian Alone'], row['Hawaiian or Pacific Islander Alone'],
                        row['Two or More Races'], row['Not Hispanic'], row['Hispanic']
                    ))
                
                conn.commit()
                # Verify the data insertion
                verify_insertion(cur)
                
    except Exception as error:
        print(f"Error: {error}")

def verify_insertion(cursor):
    """Query the data to verify insertion."""
    
    cursor.execute("SELECT * FROM ethnic_data")
    rows = cursor.fetchall()
    for row in rows:
        print(row)

if __name__ == '__main__':
    main()