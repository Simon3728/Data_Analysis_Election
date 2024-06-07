import configparser
import pandas as pd
from sqlalchemy import create_engine

def read_config(config_file='config.ini'):
    config = configparser.ConfigParser()
    config.read(config_file)
    db_params = {key: (int(value) if key == 'port' else value) for key, value in config['postgresql'].items()}
    return db_params

def create_db_engine(db_params):
    connection_string = f"postgresql://{db_params['user']}:{db_params['password']}@{db_params['host']}:{db_params['port']}/{db_params['dbname']}"
    engine = create_engine(connection_string)
    return engine

def execute_query(engine, query):
    df = pd.read_sql(query, engine)
    return df

def get_election_data(engine, year):
    query = f"""
        SELECT state, year, republican, total
        FROM election_results
        WHERE year = {year}
    """
    df = execute_query(engine, query)
    df['republican_percent'] = df['republican'] / df['total'] * 100
    return df[['state', 'year', 'republican_percent']]

def get_population_data(engine, year):
    # If the current year is 2020, get the population data from 2019
    population_year = year - 1 if year == 2020 else year
    query = f"""
        SELECT name AS state, year, total_population, population_18_24, population_25_44, population_45_64, population_65_plus
        FROM population_data
        WHERE year = {population_year}
    """
    df = execute_query(engine, query)
    df['age_18_24_percent'] = df['population_18_24'] / df['total_population'] * 100
    df['age_25_44_percent'] = df['population_25_44'] / df['total_population'] * 100
    df['age_45_64_percent'] = df['population_45_64'] / df['total_population'] * 100
    df['age_65_plus_percent'] = df['population_65_plus'] / df['total_population'] * 100

    # If the current year is 2020, set the year to 2020 in the population data
    if year == 2020:
        df['year'] = 2020

    return df[['state', 'year', 'total_population', 'age_18_24_percent', 'age_25_44_percent', 'age_45_64_percent', 'age_65_plus_percent']]

def get_gdp_data(engine, year):
    query = f"""
        SELECT state, year, total_gpd
        FROM gdp_data
        WHERE year = {year}
    """
    return execute_query(engine, query)

def get_unemployment_data(engine, year):
    query = f"""
        SELECT state, year, unemployment
        FROM unemployment_data
        WHERE year = {year}
    """
    return execute_query(engine, query)

def get_education_data(engine):
    query = f"""
        SELECT state, total_college_finishers_2000, total_college_finishers_2008, total_college_finishers_2017
        FROM education_data
    """
    return execute_query(engine, query)

def get_urban_data(engine):
    query = f"""
        SELECT state, total_college_finishers_2000, total_college_finishers_2008, total_college_finishers_2017
        FROM education_data
    """
    return execute_query(engine, query)

def merge_data(election_df, population_df, gdp_df, unemployment_df, education_df, year):
    # Select the appropriate education data column based on the year
    if year <= 2005:
        education_df['college_finishers'] = education_df['total_college_finishers_2000']*100
    elif year <= 2013:
        education_df['college_finishers'] = education_df['total_college_finishers_2008']*100
    else:
        education_df['college_finishers'] = education_df['total_college_finishers_2017']*100
    
    education_df = education_df[['state', 'college_finishers']]
    education_df['year'] = year
    
    # Merge dataframes on 'state' and 'year'
    merged_df = election_df.merge(population_df, on=['state', 'year'], how='inner')
    merged_df = merged_df.merge(gdp_df, on=['state', 'year'], how='inner')
    merged_df = merged_df.merge(unemployment_df, on=['state', 'year'], how='inner')
    merged_df = merged_df.merge(education_df, on=['state', 'year'], how='inner')
    
    # Calculate GDP per capita
    merged_df['gdp_per_capita'] = (merged_df['total_gpd'] * 1_000_000) / merged_df['total_population']
    
    # Rename columns
    final_df = merged_df.rename(columns={
        'unemployment': 'unemployment_rate'
    })
    
    # Drop rows with any NaN values
    final_df = final_df.dropna()
    
    # Round the columns to specified digits
    final_df['gdp_per_capita'] = final_df['gdp_per_capita'].round(1)
    final_df['age_18_24_percent'] = final_df['age_18_24_percent'].round(1)
    final_df['age_25_44_percent'] = final_df['age_25_44_percent'].round(1)
    final_df['age_45_64_percent'] = final_df['age_45_64_percent'].round(1)
    final_df['age_65_plus_percent'] = final_df['age_65_plus_percent'].round(1)
    final_df['college_finishers'] = final_df['college_finishers'].round(1)
    final_df['republican_percent'] = final_df['republican_percent'].round(2)
    
    return final_df[['state', 'year', 'gdp_per_capita', 'unemployment_rate', 'age_18_24_percent', 'age_25_44_percent', 'age_45_64_percent', 'age_65_plus_percent', 'republican_percent', 'college_finishers']]

def get_combined_data(engine, years):
    combined_df = pd.DataFrame()
    education_df = get_education_data(engine)
    
    for year in years:
        try:
            election_df = get_election_data(engine, year)
            population_df = get_population_data(engine, year)
            gdp_df = get_gdp_data(engine, year)
            unemployment_df = get_unemployment_data(engine, year)
            
            # Only merge data if all dataframes have data
            if not election_df.empty and not population_df.empty and not gdp_df.empty and not unemployment_df.empty:
                year_df = merge_data(election_df, population_df, gdp_df, unemployment_df, education_df, year)
                combined_df = pd.concat([combined_df, year_df], ignore_index=True)
        except Exception as e:
            print(f"Error processing data for year {year}: {e}")
    
    return combined_df

def main():
    # Define the years of interest
    years = [2000, 2004, 2008, 2012, 2016, 2020]

    # Read configuration and create database engine
    db_params = read_config()
    engine = create_db_engine(db_params)

    # Get combined data for the specified years
    final_df = get_combined_data(engine, years)

    # Display the final dataframe
    print(final_df)

if __name__ == "__main__":
    main()
