"""
This script connects to a PostgreSQL database and retrieves urbanization data to validate the accuracy of urbanization percentages.
It checks if the urbanization percentages are within the expected range (0 to 1) and are float numbers.
Additionally, it checks for any NULL values in the data and prints detailed information about any discrepancies found.
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
    column_names = ["id", "state", "urban_2000", "urban_2010"]

    try:
        with psycopg2.connect(**db_params) as conn:
            with conn.cursor() as cur:
                # Fetch the urban data
                rows = fetch_urban_data(cur)
                
                # Validate the urban data
                percentage_verify = validate_urban_data(rows)
                
                if percentage_verify:
                    print("Invalid Percentage Entries:")
                    for entry in percentage_verify:
                        id, state, invalid_fields = entry
                        print(f"ID: {id}, State: {state}")
                        for index, value in invalid_fields:
                            column_name = column_names[index]
                            print(f"Column Name: {column_name}, Value: {value}")
                else:
                    print("All percentage values are valid.")

                # Check for NULL values
                null_entries = check_for_null_values(rows)

                # Print the entries with NULL values
                for entry in null_entries:
                    print(f"Null Value Entry - ID: {entry[0]}, Name: {entry[1]}, Issue: {entry[3]}")

    except Exception as e:
        print(f"An error occurred: {e}")

def fetch_urban_data(cur):
    """
    Fetch urbanization data from the database.
    """
    cur.execute("""
        SELECT 
            id, state, urban_2000, urban_2010
        FROM urbanization_data
    """)
    return cur.fetchall()

def validate_urban_data(rows):
    """
    Validate if all data except for 'id' and 'state' are percentages between 0 and 1 and are float numbers.
    """
    invalid_entries = []

    for row in rows:
        id, state = row[0], row[1]
        invalid_fields = []

        for index, value in enumerate(row[2:], start=2):
            if not isinstance(value, float) or not (0 <= value <= 1):
                invalid_fields.append((index, value))

        if invalid_fields:
            invalid_entries.append((id, state, invalid_fields))

    return invalid_entries

def check_for_null_values(rows):
    """
    Check for NULL and NaN values in the urban data entries.
    """
    null_entries = []

    for row in rows:
        if any(col is None or (isinstance(col, float) and math.isnan(col)) for col in row):
            null_entries.append((row[0], row[1], 'NULL or NaN values found in entry'))

    return null_entries

if __name__ == "__main__":
    main()