"""
This script connects to a PostgreSQL database and retrieves unemployment data to validate the accuracy of unemployment rates.
It checks if the unemployment rates fall within the expected range (0 to 1) and are float numbers.
Critical mismatches and NULL values are identified and printed with detailed information about the discrepancies.
"""

import psycopg2
import configparser
import math 


def main():
    # Read the configuration file
    config = configparser.ConfigParser()
    config.read('config.ini')

    # Get database connection parameters
    db_params = {key: (int(value) if key == 'port' else value) for key, value in config['postgresql'].items()}

    # Column names
    column_names = ["id", "state", "year", "unemployment"]

    try:
        with psycopg2.connect(**db_params) as conn:
            with conn.cursor() as cur:
                # Fetch the unemployment data
                rows = fetch_unemployment_data(cur)
                
                # Validate the unemployment data
                percentage_verify = validate_unemployment_data(rows)
                
                if percentage_verify:
                    print("Invalid Percentage Entries:")
                    for entry in percentage_verify:
                        id, state, invalid_fields = entry
                        for year, index, value in invalid_fields:
                            column_name = column_names[index]
                            print(f"ID: {id}, State: {state}, Year: {year}, Column Name: {column_name}, Value: {value}")
                else:
                    print("All percentage values are valid.")

                # Check for NULL values
                null_entries = check_for_null_values(rows)

                # Print the entries with NULL values
                for entry in null_entries:
                    print(f"Null Value Entry - ID: {entry[0]}, Name: {entry[1]}, Issue: {entry[3]}")

    except Exception as e:
        print(f"An error occurred: {e}")

def fetch_unemployment_data(cur):
    """
    Fetch unemployment data from the database.
    """
    cur.execute("""
        SELECT 
            id, state, year, unemployment
        FROM unemployment_data
    """)
    return cur.fetchall()

def validate_unemployment_data(rows):
    """
    Validate the unemployment data to check if its between 0 and 1
    """
    invalid_entries = []

    for row in rows:
        id, state, year, unemployment = row
        invalid_fields = []

        if not isinstance(unemployment, float) or not (0 <= unemployment <= 1):
            invalid_fields.append((year, 3, unemployment))  

        if invalid_fields:
            invalid_entries.append((id, state, invalid_fields))

    return invalid_entries


def check_for_null_values(rows):
    """
    Check for NULL and NaN values in the unemployment data entries.
    """
    null_entries = []

    for row in rows:
        if any(col is None or (isinstance(col, float) and math.isnan(col)) for col in row):
            null_entries.append((row[0], row[1], row[2], 'NULL or NaN values found in entry'))

    return null_entries

if __name__ == "__main__":
    main()