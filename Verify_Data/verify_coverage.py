"""
This script connects to a PostgreSQL database and retrieves various data sets to validate their coverage across different US states and years.
It checks for NULL values, verifies state coverage, and ensures that data for each state spans the expected year range.
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

    try:
        with psycopg2.connect(**db_params) as conn:
            with conn.cursor() as cur:
                
                # Data which will only be checked for State
                data_tables_state = [
                    ("Education Data", fetch_education_data(cur)),
                    ("Election Data", fetch_election_data(cur)),
                    ("Urban Data", fetch_urban_data(cur))
                ]

                # Data which will be checked for State and Year
                data_tables_state_year = [
                    ("Population Data", fetch_population_data(cur)),
                    ("Ethnic Data", fetch_ethnic_data(cur)),
                    ("GDP Data", fetch_gdp_data(cur)),
                    ("Unemployment Data", fetch_unemployment_data(cur)),
                ]

                for name, row in data_tables_state_year:
                    # Check for NULL values
                    print("------------------------------------------------------------------")
                    print(f"Check: {name}")

                    null_entries = check_for_null_values(row)

                    # Check for state coverage and year range
                    coverage_info, missing_states = check_state_year_coverage(row)

                    # Print the entries with NULL values
                    print("Check Null Values: ")
                    for entry in null_entries:
                        print(f"Null Value Entry - ID: {entry[0]}, Name: {entry[1]}, Year: {entry[2]}, Issue: {entry[3]}")

                    # Print state coverage information
                    print("\nState Year Coverage: ")
                    for info in coverage_info:
                        print(info)

                    # Print missing states
                    print("\nMissing States: ")
                    for state in missing_states:
                        print(state)

                for name, row in data_tables_state:
                    # Check for NULL values
                    print("------------------------------------------------------------------")
                    print(f"Check: {name}")

                    null_entries = check_for_null_values(row)

                    # Check for state coverage
                    missing_states = check_state_coverage(row)

                    # Print the entries with NULL values
                    print("Check Null Values: ")
                    for entry in null_entries:
                        print(f"Null Value Entry - ID: {entry[0]}, Name: {entry[1]}, Issue: {entry[2]}")

                    # Print missing states
                    print("\nMissing States: ")
                    for state in missing_states:
                        print(state)

    except Exception as e:
        print(f"An error occurred: {e}")


def fetch_urban_data(cur):
    """
    Fetch urban_data data from the database.
    """
    cur.execute("""
        SELECT 
            id, state
        FROM urbanization_data
    """)
    return cur.fetchall()

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

def fetch_gdp_data(cur):
    """
    Fetch gdp data from the database.
    """
    cur.execute("""
        SELECT 
            id, state, year
        FROM gdp_data
    """)
    return cur.fetchall()

def fetch_ethnic_data(cur):
    """
    Fetch ethnic data from the database.
    """
    cur.execute("""
        SELECT 
            id, state, year
        FROM ethnic_data
    """)
    return cur.fetchall()

def fetch_election_data(cur):
    """
    Fetch election data from the database.
    """
    cur.execute("""
        SELECT 
            id, state, year
        FROM election_results
    """)
    return cur.fetchall()

def fetch_education_data(cur):
    """
    Fetch education data from the database.
    """
    cur.execute("""
        SELECT 
            id, state
        FROM education_data
    """)
    return cur.fetchall()

def fetch_population_data(cur):
    """
    Fetch population data from the database.
    """
    cur.execute("""
        SELECT 
            id, name, year
        FROM population_data
    """)
    return cur.fetchall()

def check_for_null_values(rows):
    """
    Check for NULL and NaN values in the data entries.
    """
    null_entries = []

    for row in rows:
        if any(col is None or (isinstance(col, float) and math.isnan(col)) for col in row):
            null_entries.append((row[0], row[1], row[2], 'NULL or NaN values found in entry'))

    return null_entries

def check_for_null_values2(rows):
    """
    Check for NULL and NaN values in the data entries.
    """
    null_entries = []

    for row in rows:
        if any(col is None or (isinstance(col, float) and math.isnan(col)) for col in row):
            null_entries.append((row[0], row[1], 'NULL or NaN values found in entry'))

    return null_entries

def check_state_year_coverage(rows):
    """
    Check data for coverage across all US states and determine the range of years available for each state.
    Also, verify if all years in the range are present.
    """
    VALID_STATES = [
        'Alabama', 'Alaska', 'Arizona', 'Arkansas', 'California', 'Colorado', 'Connecticut', 'Delaware', 'Florida',
        'Georgia', 'Hawaii', 'Idaho', 'Illinois', 'Indiana', 'Iowa', 'Kansas', 'Kentucky', 'Louisiana', 'Maine',
        'Maryland', 'Massachusetts', 'Michigan', 'Minnesota', 'Mississippi', 'Missouri', 'Montana', 'Nebraska',
        'Nevada', 'New Hampshire', 'New Jersey', 'New Mexico', 'New York', 'North Carolina', 'North Dakota', 'Ohio',
        'Oklahoma', 'Oregon', 'Pennsylvania', 'Rhode Island', 'South Carolina', 'South Dakota', 'Tennessee', 'Texas',
        'Utah', 'Vermont', 'Virginia', 'Washington', 'West Virginia', 'Wisconsin', 'Wyoming'
    ]

    state_years = {state: set() for state in VALID_STATES}

    for row in rows:
        state, year = row[1], row[2]
        if state in state_years:
            state_years[state].add(year)

    coverage_info = []
    missing_states = []

    for state, years in state_years.items():
        if years:
            min_year, max_year = min(years), max(years)
            full_range = set(range(min_year, max_year + 1))
            missing_years = full_range - years
            if missing_years:
                coverage_info.append(f"{state}, {min_year}-{max_year} (missing years: {sorted(missing_years)})")
            else:
                coverage_info.append(f"{state}, {min_year}-{max_year}")
        else:
            missing_states.append(state)

    return coverage_info, missing_states

def check_state_coverage(rows):
    """
    Check data for coverage across all US states.
    """
    VALID_STATES = [
        'Alabama', 'Alaska', 'Arizona', 'Arkansas', 'California', 'Colorado', 'Connecticut', 'Delaware', 'Florida',
        'Georgia', 'Hawaii', 'Idaho', 'Illinois', 'Indiana', 'Iowa', 'Kansas', 'Kentucky', 'Louisiana', 'Maine',
        'Maryland', 'Massachusetts', 'Michigan', 'Minnesota', 'Mississippi', 'Missouri', 'Montana', 'Nebraska',
        'Nevada', 'New Hampshire', 'New Jersey', 'New Mexico', 'New York', 'North Carolina', 'North Dakota', 'Ohio',
        'Oklahoma', 'Oregon', 'Pennsylvania', 'Rhode Island', 'South Carolina', 'South Dakota', 'Tennessee', 'Texas',
        'Utah', 'Vermont', 'Virginia', 'Washington', 'West Virginia', 'Wisconsin', 'Wyoming', 'United States'
    ]

    # Create a set to track states found in the rows
    found_states = set()

    # Iterate over the rows and add the state to the set of found states
    for row in rows:
        state = row[1]  
        found_states.add(state)

    # Determine the missing states by subtracting the found states from the valid states
    missing_states = set(VALID_STATES) - found_states

    return list(missing_states)

if __name__ == "__main__":
    main()