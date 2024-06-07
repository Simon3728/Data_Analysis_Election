import configparser
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.animation import FuncAnimation, PillowWriter
from sqlalchemy import create_engine

# Read the configuration file
config = configparser.ConfigParser()
config.read('config.ini')

# Define required keys
required_keys = {'user', 'password', 'host', 'port', 'database'}

# Get database connection parameters and check if all required keys are present
db_params = {key: (int(value) if key == 'port' else value) for key, value in config['postgresql'].items()}
if not required_keys.issubset(db_params):
    missing_keys = required_keys - db_params.keys()
    raise KeyError(f"Missing required configuration keys: {missing_keys}")

# Create the database URL
db_url = f"postgresql://{db_params['user']}:{db_params['password']}@{db_params['host']}:{db_params['port']}/{db_params['dbname']}"

# Establish the database connection using SQLAlchemy
engine = create_engine(db_url)

# Fetch GDP data
gdp_query = """
    SELECT 
        id, state, year, total_gpd
    FROM gdp_data
"""
gdp_data = pd.read_sql_query(gdp_query, engine)

# Fetch population data
population_query = """
    SELECT 
        id, name as state, year, total_population
    FROM population_data
"""
population_data = pd.read_sql_query(population_query, engine)

# Merge the GDP and population data on state and year
merged_data = pd.merge(gdp_data, population_data, on=['state', 'year'])

# Calculate GDP per person
merged_data['gdp_per_person'] = (merged_data['total_gpd'] * 1e6) / merged_data['total_population']

# Function to create a horizontal bar chart for a given year
def plot_year(year):
    data_to_plot = merged_data[merged_data['year'] == year].sort_values(by='gdp_per_person', ascending=True)
    plt.barh(data_to_plot['state'], data_to_plot['gdp_per_person'])
    plt.xlabel('GDP per Person ($)')
    plt.title(f'GDP per Person by State in {year}')
    plt.xlim(0, merged_data['gdp_per_person'].max() * 1.1)

# Create the animation
fig, ax = plt.subplots(figsize=(12, 8))

def update(year):
    ax.clear()
    plot_year(year)

years = range(2000, 2020)
ani = FuncAnimation(fig, update, frames=years, repeat=False)

# Save the animation as a GIF
ani.save('gdp_per_person_by_state.gif', writer=PillowWriter(fps=1))
