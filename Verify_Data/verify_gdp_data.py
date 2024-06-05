"""
This script connects to a PostgreSQL database and retrieves GDP data to validate the accuracy of GDP distributions by state and year.
It checks if the sum of GDP values for all states in a given year roughly matches the total GDP value for the United States.
Critical mismatches are identified and printed with detailed information about the discrepancies.
"""

import psycopg2
import configparser
import math
from collections import defaultdict

def main():
    tolerance = 0.02 # 2% Tolerance

    # Read the configuration file
    config = configparser.ConfigParser()
    config.read('config.ini')

    # Get database connection parameters
    db_params = {key: (int(value) if key == 'port' else value) for key, value in config['postgresql'].items()}

    try:
        with psycopg2.connect(**db_params) as conn:
            with conn.cursor() as cur:
                # Fetch the GDP data

                rows = fetch_gdp_data(cur)
                # Validate the GDP data
                critical_entries = validate_gdp_data(rows, tolerance)

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
                    
def fetch_gdp_data(cur):
    """
    Fetch GDP data from the database.
    """
    cur.execute("""
        SELECT 
            id, state, year, total_gpd
        FROM gdp_data
    """)
    return cur.fetchall()

def validate_gdp_data(rows, tolerance):
    """
    Validate the GDP data to check for mismatches in GDP sums across states.
    """

    # Dictionary to hold the GDP sums for each year
    state_gdp_sums = defaultdict(float)
    us_gdp = {}

    # Calculate the sum of GDP for each state and the GDP for 'United States'
    for row in rows:
        _, state, year, total_gdp = row
        if state == 'United States':
            us_gdp[year] = total_gdp
        else:
            state_gdp_sums[year] += total_gdp

    critical_entries = []

    # Compare the state GDP sums to the 'United States' GDP values
    for year, state_gdp_sum in state_gdp_sums.items():
        if year in us_gdp:
            us_gdp_value = us_gdp[year]
            if not (us_gdp_value * (1 - tolerance) <= state_gdp_sum <= us_gdp_value * (1 + tolerance)):
                issue = f"State GDP sum {state_gdp_sum} not within tolerance of US GDP {us_gdp_value} for year {year}"
                critical_entries.append((None, 'United States', year, issue))

    return critical_entries

def check_for_null_values(rows):
    """
    Check for NULL and NaN values in the GDP data entries.
    """
    null_entries = []

    for row in rows:
        if any(col is None or (isinstance(col, float) and math.isnan(col)) for col in row):
            null_entries.append((row[0], row[1], 'NULL or NaN values found in entry'))

    return null_entries

if __name__ == "__main__":
    main()