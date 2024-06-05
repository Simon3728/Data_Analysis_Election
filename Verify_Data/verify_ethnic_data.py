"""
This script connects to a PostgreSQL database and retrieves ethnic data to validate the accuracy of population distributions by ethnicity.
It checks if the sum of hispanics and non-hispanics roughly matches the total population for each entry.
Critical mismatches are identified and printed with detailed information about the discrepancies.
"""

import psycopg2
import configparser
import math

def main():
    # Set the tolerance for the population checks
    tolerance = 0.001  # 0.1%

    # Read the configuration file
    config = configparser.ConfigParser()
    config.read('config.ini')

    # Get database connection parameters
    db_params = {key: (int(value) if key == 'port' else value) for key, value in config['postgresql'].items()}

    try:
        with psycopg2.connect(**db_params) as conn:
            with conn.cursor() as cur:
                # Fetch the ethnic data
                rows = fetch_ethnic_data(cur)
                
                # Validate the ethnic data
                critical_entries = validate_ethnic_data(rows, tolerance)

                # Check for NULL values
                null_entries = check_for_null_values(rows)

                # Print the critical entries with detailed mismatch data
                for entry in critical_entries:
                    print(f"Critical Entry - ID: {entry[0]}, Name: {entry[1]}, Year: {entry[2]}, Issue: {entry[3]}")

                # Print the entries with NULL values
                for entry in null_entries:
                    print(f"Null Value Entry - ID: {entry[0]}, Name: {entry[1]}, Year: {entry[2]}, Issue: {entry[3]}")

    except Exception as e:
        print(f"An error occurred: {e}")

def roughly_equal(a, b, tolerance):
    """
    Check if two values are roughly equal within a given tolerance.
    """
    return abs(a - b) <= tolerance * max(a, b)

def fetch_ethnic_data(cur):
    """
    Fetch ethnic data from the database.
    """
    cur.execute("""
        SELECT 
            id, state, year, total_population, 
            white_alone, black_alone, american_indian_alone, 
            asian_alone, pacific_islander_alone, two_or_more_races, 
            not_hispanic, hispanic 
        FROM ethnic_data
    """)
    return cur.fetchall()

def validate_ethnic_data(rows, tolerance):
    """
    Validate the ethnic data to check for mismatches in Hispanic population and Non-Hispanic population.
    """
    critical_entries = []

    for row in rows:
        # Check if hispanic + not_hispanic equals total_population
        if not roughly_equal(row[10] + row[11], row[3], tolerance):
            critical_entries.append((id, row[1], row[2], f'Hispanic population mismatch: hispanic={row[11]}, not_hispanic={row[10]}, total_population={row[3]}'))

    return critical_entries

def check_for_null_values(rows):
    """
    Check for NULL and NaN values in the ethnic data entries.
    """
    null_entries = []

    for row in rows:
        if any(col is None or (isinstance(col, float) and math.isnan(col)) for col in row):
            null_entries.append((row[0], row[1], row[2], 'NULL or NaN values found in entry'))

    return null_entries

if __name__ == "__main__":
    main()