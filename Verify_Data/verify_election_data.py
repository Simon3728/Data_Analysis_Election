"""
This script connects to a PostgreSQL database and retrieves election data to validate the accuracy of voting population distributions.
It checks if the sum of different voting groups roughly matches the total voting population for each entry.
"""

import psycopg2
import configparser
import math

def main():
    # Set the tolerance for the election check
    tolerance = 0.0001 # 0.01%

    # Read the configuration file
    config = configparser.ConfigParser()
    config.read('config.ini')

    # Get database connection parameters
    db_params = {key: (int(value) if key == 'port' else value) for key, value in config['postgresql'].items()}

    try:
        with psycopg2.connect(**db_params) as conn:
            with conn.cursor() as cur:
                # Fetch the election data
                rows = fetch_election_data(cur)
                
                # Validate the election data
                critical_entries = validate_election_data(rows, tolerance)

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

def fetch_election_data(cur):
    """
    Fetch election data from the database.
    """
    cur.execute("""
        SELECT 
            id, state, year, republican, democratic, others, total
        FROM election_results
    """)
    return cur.fetchall()

def validate_election_data(rows, tolerance):
    """
    Validate the election data to check for mismatches in total votes.
    """
    critical_entries = []

    for row in rows:
        # Calculate the sum of the votes
        votes_sum = (row[3] + row[4] + row[5])
        # Check if age groups sum roughly equals total_population
        if not roughly_equal(votes_sum, row[6], tolerance):
            critical_entries.append((id, row[1], row[2], f'Age groups sum mismatch: calculated sum={votes_sum}, total_population={row[6]}'))

    return critical_entries

def check_for_null_values(rows):
    """
    Check for NULL and NaN values in the election data entries.
    """
    null_entries = []

    for row in rows:
        if any(col is None or (isinstance(col, float) and math.isnan(col)) for col in row):
            null_entries.append((row[0], row[1], row[2], 'NULL or NaN values found in entry'))

    return null_entries

if __name__ == "__main__":
    main()