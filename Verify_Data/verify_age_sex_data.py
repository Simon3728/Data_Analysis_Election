"""
This script connects to a PostgreSQL database and retrieves population data to validate the accuracy of age group and gender population distributions. 
It checks if the sum of different age groups and the sum of male and female populations roughly match the total population for each entry. 
Critical mismatches are identified and printed with detailed information about the discrepancies. Additionally, it checks for any NULL values in the data.
"""

import psycopg2
import configparser
import math 

def main():
    # Set the tolerance for the population checks
    tolerance = 0.005  # 0.5%

    # Read the configuration file
    config = configparser.ConfigParser()
    config.read('config.ini')

    # Get database connection parameters
    db_params = {key: (int(value) if key == 'port' else value) for key, value in config['postgresql'].items()}

    try:
        with psycopg2.connect(**db_params) as conn:
            with conn.cursor() as cur:
                # Fetch the population data
                rows = fetch_population_data(cur)
                
                # Validate the population data
                critical_entries = validate_population_data(rows, tolerance)

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

def fetch_population_data(cur):
    """
    Fetch population data from the database.
    """
    cur.execute("""
        SELECT id, name, year, total_population, 
               population_0_4, population_5_17, population_18_24, 
               population_25_44, population_45_64, population_65_plus, 
               male_population, female_population 
        FROM population_data
    """)
    return cur.fetchall()

def validate_population_data(rows, tolerance):
    """
    Validate the population data to check for mismatches in age groups sum and gender population.
    """
    critical_entries = []

    for row in rows:
        id, name, year, total_population, population_0_4, population_5_17, population_18_24, \
        population_25_44, population_45_64, population_65_plus, male_population, female_population = row

        # Calculate the sum of the age groups
        age_groups_sum = (population_0_4 + population_5_17 + population_18_24 +
                          population_25_44 + population_45_64 + population_65_plus)

        # Check if age groups sum roughly equals total_population
        if not roughly_equal(age_groups_sum, total_population, tolerance):
            critical_entries.append((id, name, year, f'Age groups sum mismatch: calculated sum={age_groups_sum}, total_population={total_population}'))

        # Check if male_population + female_population equals total_population
        if not roughly_equal(male_population + female_population, total_population, tolerance):
            critical_entries.append((id, name, year, f'Gender population mismatch: male_population={male_population}, female_population={female_population}, total_population={total_population}'))

    return critical_entries

def check_for_null_values(rows):
    """
    Check for NULL and NaN values in the population data entries.
    """
    null_entries = []

    for row in rows:
        if any(col is None or (isinstance(col, float) and math.isnan(col)) for col in row):
            null_entries.append((row[0], row[1], row[2], 'NULL or NaN values found in entry'))

    return null_entries

if __name__ == "__main__":
    main()
