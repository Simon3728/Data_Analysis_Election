import configparser
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.animation import FuncAnimation, PillowWriter
from sqlalchemy import create_engine

"""
This script connects to a PostgreSQL database, fetches GDP and population data, calculates GDP per person, 
and creates an animated horizontal bar chart showing GDP per person by state from 2000 to 2019. 
The animation is saved as a GIF file.
"""

def read_config(config_file):
    """Reads the database configuration from a config file."""
    config = configparser.ConfigParser()
    config.read(config_file)
    return config

def get_db_params(config):
    """Gets database connection parameters from the config and ensures required keys are present."""
    required_keys = {'user', 'password', 'host', 'port', 'dbname'}
    if 'postgresql' not in config:
        raise KeyError("Missing [postgresql] section in configuration file")
    db_params = {key: (int(value) if key == 'port' else value) for key, value in config['postgresql'].items()}
    missing_keys = required_keys - db_params.keys()
    if missing_keys:
        raise KeyError(f"Missing required configuration keys: {missing_keys}")
    return db_params

def create_db_engine(db_params):
    """Creates a SQLAlchemy engine for the database connection."""
    db_url = f"postgresql://{db_params['user']}:{db_params['password']}@{db_params['host']}:{db_params['port']}/{db_params['dbname']}"
    engine = create_engine(db_url)
    return engine

def fetch_data(engine, query):
    """Fetches data from the database using the provided SQL query."""
    return pd.read_sql_query(query, engine)

def merge_data(gdp_data, population_data):
    """Merges GDP and population data on state and year, and calculates GDP per person."""
    merged_data = pd.merge(gdp_data, population_data, on=['state', 'year'])
    merged_data['gdp_per_person'] = (merged_data['total_gpd'] * 1e6) / merged_data['total_population']
    return merged_data

def plot_year(data, year, ax):
    """Creates a horizontal bar chart for a given year."""
    data_to_plot = data[data['year'] == year].sort_values(by='gdp_per_person', ascending=True)
    ax.barh(data_to_plot['state'], data_to_plot['gdp_per_person'])
    ax.set_xlabel('GDP per Person ($)')
    ax.set_title(f'GDP per Person by State in {year}')
    ax.set_xlim(0, data['gdp_per_person'].max() * 1.1)

def update_plot(year, data, ax):
    """Updates the plot for the animation."""
    ax.clear()
    plot_year(data, year, ax)

def create_animation(data, years, output_file):
    """Creates and saves the animation as a GIF."""
    fig, ax = plt.subplots(figsize=(12, 8))
    ani = FuncAnimation(fig, update_plot, frames=years, fargs=(data, ax), repeat=False)
    ani.save(output_file, writer=PillowWriter(fps=1))

def main():
    """Main function to execute the script."""
    config_file = 'config.ini'
    config = read_config(config_file)
    db_params = get_db_params(config)
    engine = create_db_engine(db_params)

    gdp_query = """
        SELECT 
            id, state, year, total_gpd
        FROM gdp_data
    """
    gdp_data = fetch_data(engine, gdp_query)

    population_query = """
        SELECT 
            id, name as state, year, total_population
        FROM population_data
    """
    population_data = fetch_data(engine, population_query)

    merged_data = merge_data(gdp_data, population_data)

    years = range(2000, 2020)
    output_file = 'Visualize_Data/Plots/gdp_per_person_by_state.gif'
    create_animation(merged_data, years, output_file)

if __name__ == "__main__":
    main()
