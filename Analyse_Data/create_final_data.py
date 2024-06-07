"""
This script connects to a PostgreSQL database, retrieves various datasets,
processes them, and merges them into a final DataFrame. The data includes election results, population data,
GDP, unemployment rates, education statistics, and urbanization data for specified years and states.
"""

import configparser
import pandas as pd
from sqlalchemy import create_engine

def read_config(config_file='config.ini'):
    """
    Reads database configuration from a config file and returns the parameters as a dictionary.
    """
    config = configparser.ConfigParser()
    config.read(config_file)
    db_params = {key: (int(value) if key == 'port' else value) for key, value in config['postgresql'].items()}
    return db_params

def create_db_engine(db_params):
    """
    Creates a SQLAlchemy engine using the provided database parameters.
    """
    connection_string = f"postgresql://{db_params['user']}:{db_params['password']}@{db_params['host']}:{db_params['port']}/{db_params['dbname']}"
    engine = create_engine(connection_string)
    return engine

def execute_query(engine, query):
    """
    Executes a SQL query using the provided engine and returns the result as a DataFrame.
    """
    df = pd.read_sql(query, engine)
    return df

def get_election_data(engine, year):
    """
    Retrieves election data for a specified year from the database and calculates the republican vote percentage.
    """
    query = f"""
        SELECT state, year, republican, total
        FROM election_results
        WHERE year = {year}
    """
    df = execute_query(engine, query)
    df['republican_percent'] = df['republican'] / df['total'] * 100
    return df[['state', 'year', 'republican_percent']]

def get_population_data(engine, year):
    """
    Retrieves population data for a specified year from the database, processes it to calculate age group percentages,
    and adjusts the year if 2020.
    """
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

    if year == 2020:
        df['year'] = 2020

    return df[['state', 'year', 'total_population', 'age_18_24_percent', 'age_25_44_percent', 'age_45_64_percent', 'age_65_plus_percent']]

def get_gdp_data(engine, year):
    """
    Retrieves GDP data for a specified year from the database.
    """
    query = f"""
        SELECT state, year, total_gpd
        FROM gdp_data
        WHERE year = {year}
    """
    return execute_query(engine, query)

def get_unemployment_data(engine, year):
    """
    Retrieves unemployment data for a specified year from the database.
    """
    query = f"""
        SELECT state, year, unemployment
        FROM unemployment_data
        WHERE year = {year}
    """
    return execute_query(engine, query)

def get_education_data(engine, year):
    """
    Retrieves education data from the database and selects the appropriate education statistics column based on the year.
    """
    query = f"""
        SELECT state, total_college_finishers_2000, total_college_finishers_2008, total_college_finishers_2017
        FROM education_data
    """
    df = execute_query(engine, query)
    
    if year <= 2005:
        df['college_finishers'] = df['total_college_finishers_2000'] * 100
    elif year <= 2013:
        df['college_finishers'] = df['total_college_finishers_2008'] * 100
    else:
        df['college_finishers'] = df['total_college_finishers_2017'] * 100
    
    df = df[['state', 'college_finishers']]
    df['year'] = year
    
    return df

def get_urbanization_data(engine, year):
    """
    Retrieves urbanization data from the database and selects the appropriate urbanization statistics column based on the year.
    """
    query = f"""
        SELECT state, urban_2000, urban_2010
        FROM urbanization_data
    """
    df = execute_query(engine, query)
    
    if year <= 2005:
        df['urban_population'] = df['urban_2000'] * 100
    else:
        df['urban_population'] = df['urban_2010'] * 100
    
    df = df[['state', 'urban_population']]
    df['year'] = year
    
    return df

def merge_data(election_df, population_df, gdp_df, unemployment_df, education_df, urbanization_df, year):
    """
    Merges multiple dataframes on 'state' and 'year', calculates GDP per capita, renames columns, and rounds values.
    """
    merged_df = election_df.merge(population_df, on=['state', 'year'], how='inner')
    merged_df = merged_df.merge(gdp_df, on=['state', 'year'], how='inner')
    merged_df = merged_df.merge(unemployment_df, on=['state', 'year'], how='inner')
    merged_df = merged_df.merge(education_df, on=['state', 'year'], how='inner')
    merged_df = merged_df.merge(urbanization_df, on=['state', 'year'], how='inner')
    
    merged_df['gdp_per_capita'] = (merged_df['total_gpd'] * 1_000_000) / merged_df['total_population']
    
    final_df = merged_df.rename(columns={
        'unemployment': 'unemployment_rate'
    })
    
    final_df = final_df.dropna()
    
    final_df['gdp_per_capita'] = final_df['gdp_per_capita'].round(0)
    final_df['age_18_24_percent'] = final_df['age_18_24_percent'].round(1)
    final_df['age_25_44_percent'] = final_df['age_25_44_percent'].round(1)
    final_df['age_45_64_percent'] = final_df['age_45_64_percent'].round(1)
    final_df['age_65_plus_percent'] = final_df['age_65_plus_percent'].round(1)
    final_df['urban_population'] = final_df['urban_population'].round(1)
    final_df['college_finishers'] = final_df['college_finishers'].round(1)
    final_df['republican_percent'] = final_df['republican_percent'].round(2)
    
    return final_df[['state', 'year', 'gdp_per_capita', 'unemployment_rate', 'college_finishers', 'urban_population', 'age_18_24_percent', 'age_25_44_percent', 'age_45_64_percent', 'age_65_plus_percent', 'republican_percent']]

def get_combined_data(engine, years):
    """
    Retrieves and combines data for multiple years, merging election, population, GDP, unemployment, education,
    and urbanization data into a single DataFrame.
    """
    combined_df = pd.DataFrame()
    
    for year in years:
        try:
            election_df = get_election_data(engine, year)
            population_df = get_population_data(engine, year)
            gdp_df = get_gdp_data(engine, year)
            unemployment_df = get_unemployment_data(engine, year)
            education_df = get_education_data(engine, year)
            urbanization_df = get_urbanization_data(engine, year)
            
            if not election_df.empty and not population_df.empty and not gdp_df.empty and not unemployment_df.empty and not education_df.empty and not urbanization_df.empty:
                year_df = merge_data(election_df, population_df, gdp_df, unemployment_df, education_df, urbanization_df, year)
                combined_df = pd.concat([combined_df, year_df], ignore_index=True)
        except Exception as e:
            print(f"Error processing data for year {year}: {e}")
    
    return combined_df

def create_final_data(years):
    """
    Reads configuration, creates a database engine, and retrieves combined data for the specified years.
    """
    db_params = read_config()
    engine = create_db_engine(db_params)
    return get_combined_data(engine, years)

