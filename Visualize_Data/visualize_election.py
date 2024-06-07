import configparser
import pandas as pd
import geopandas as gpd
import matplotlib.pyplot as plt
from sqlalchemy import create_engine

def load_db_config(config_file='config.ini'):
    """
    Load database configuration from a config file.
    """
    config = configparser.ConfigParser()
    config.read(config_file)
    db_params = {key: value for key, value in config['postgresql'].items()}
    return db_params

def create_db_engine(db_params):
    """
    Create a SQLAlchemy engine using database connection parameters.
    """
    conn_str = f"postgresql://{db_params['user']}:{db_params['password']}@{db_params['host']}:{db_params['port']}/{db_params['dbname']}"
    engine = create_engine(conn_str)
    return engine

def fetch_election_data(engine, year):
    """
    Fetch election data from the database for a specific year.
    """
    query = f"""
    SELECT 
        state, year, republican, democratic, others, total
    FROM election_results
    WHERE year = {year}
    """
    df = pd.read_sql_query(query, engine)
    return df

def load_shapefile(shapefile_path):
    """
    Load the shapefile for US states.
    """
    gdf = gpd.read_file(shapefile_path)
    return gdf

def merge_data(shapefile_gdf, election_df):
    """
    Merge shapefile GeoDataFrame with election data DataFrame.
    """
    # Determine the dominant party for each state
    election_df['dominant_party'] = election_df.apply(
        lambda row: 'republican' if row['republican'] > row['democratic'] else 'democratic',
        axis=1
    )
    
    # Calculate the margin of victory for the dominant party
    election_df['margin_of_victory'] = abs(election_df['republican'] - election_df['democratic']) / election_df['total']
    
    # Calculate the winning percentage
    election_df['winning_percentage'] = election_df.apply(
        lambda row: row['republican'] / row['total'] if row['dominant_party'] == 'republican' else row['democratic'] / row['total'],
        axis=1
    )
    
    # Ensure the shapefile GeoDataFrame has the same CRS as the election data (usually WGS84)
    shapefile_gdf = shapefile_gdf.to_crs(epsg=4326)
    
    # Convert state names to uppercase for matching
    shapefile_gdf['State_Name'] = shapefile_gdf['State_Name'].str.upper()
    election_df['state'] = election_df['state'].str.upper()
    
    # Merge the shapefile GeoDataFrame with the election results DataFrame
    merged_gdf = shapefile_gdf.merge(election_df[['state', 'dominant_party', 'margin_of_victory', 'winning_percentage']], left_on='State_Name', right_on='state', how='left')
    
    # Check for and remove invalid geometries
    merged_gdf = merged_gdf[merged_gdf.is_valid]
    
    return merged_gdf

def calculate_total_results(election_df):
    """
    Calculate the total results of the election in percentages.
    """
    total_votes = election_df['total'].sum()
    total_democratic = election_df['democratic'].sum()
    total_republican = election_df['republican'].sum()
    
    democratic_percentage = (total_democratic / total_votes) * 100
    republican_percentage = (total_republican / total_votes) * 100
    
    return democratic_percentage, republican_percentage

def plot_election_results(merged_gdf, year, democratic_percentage, republican_percentage):
    """
    Plot election results on the US map.
    """
    # Create a color map for the dominant party
    party_colors = {'republican': (1, 0, 0), 'democratic': (0, 0, 1)}  # RGB values for red and blue
    
    # Map the dominant party to the corresponding color and adjust opacity based on margin of victory
    merged_gdf['color'] = merged_gdf.apply(
        lambda row: (*party_colors[row['dominant_party']], min(1, max(0.4, row['margin_of_victory'] * 2))),
        axis=1
    )
    
    # Plot the map
    fig, ax = plt.subplots(1, 1, figsize=(15, 10))
    merged_gdf.plot(ax=ax, color=merged_gdf['color'], edgecolor='black')
    
    # Set plot title and total results
    ax.set_title(f'US Election Results by State - {year}', fontsize=15)
    plt.suptitle(f'Total Results: Democrats {democratic_percentage:.2f}%, Republicans {republican_percentage:.2f}%', fontsize=12)
    
    # Remove axes
    ax.set_axis_off()
    
    # Add legend
    handles = [plt.Line2D([0], [0], marker='o', color='w', markerfacecolor=(party_colors[party][0], party_colors[party][1], party_colors[party][2], 0.7), markersize=10, label=party.title())
               for party in party_colors]
    ax.legend(handles=handles, loc='lower left')

    # Show plot
    plt.show()

def main():
    # Set year
    year = 2020

    # Load database configuration
    db_params = load_db_config()
    
    # Create database engine
    engine = create_db_engine(db_params)
    
    # Fetch election data for the specified year
    df = fetch_election_data(engine, year)
    
    # Calculate total results
    democratic_percentage, republican_percentage = calculate_total_results(df)
    
    # Load the shapefile
    shapefile_gdf = load_shapefile('Visualize_Data/shp/States_shapefile.shp')
    
    # Merge the shapefile data with election data
    merged_gdf = merge_data(shapefile_gdf, df)
    
    # Plot the election results
    plot_election_results(merged_gdf, year, democratic_percentage, republican_percentage)

if __name__ == "__main__":
    main()
