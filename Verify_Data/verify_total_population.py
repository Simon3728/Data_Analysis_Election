import configparser
import psycopg2
import matplotlib.pyplot as plt

# Read the configuration file
config = configparser.ConfigParser()
config.read('config.ini')

# Get database connection parameters
db_params = {key: (int(value) if key == 'port' else value) for key, value in config['postgresql'].items()}

def fetch_population_data(cur, year):
    cur.execute("""
        SELECT name, total_population, population_0_4, population_5_17, population_18_24, 
               population_25_44, population_45_64, population_65_plus,
               male_population, female_population
        FROM population_data
        WHERE year = %s
    """, (year,))
    return cur.fetchone()

try:
    with psycopg2.connect(**db_params) as conn:
        with conn.cursor() as cur:
            # Fetch the population data for a specific year
            year = 2019  # Replace with the year you are interested in
            result = fetch_population_data(cur, year)

            # Validate the result
            if not result:
                raise ValueError(f"No data found for the year {year}")

            # Unpack the result
            (name, total_population, population_0_4, population_5_17, population_18_24, 
             population_25_44, population_45_64, population_65_plus,
             male_population, female_population) = result

except Exception as e:
    print(f"An error occurred: {e}")
    exit()

# Plotting the data

# Age group populations
age_groups = ['0-4', '5-17', '18-24', '25-44', '45-64', '65+']
population_counts = [population_0_4, population_5_17, population_18_24, 
                     population_25_44, population_45_64, population_65_plus]

# Create subplots
fig, axs = plt.subplots(1, 2, figsize=(18, 8))

# Bar plot for age groups
axs[0].bar(age_groups, population_counts, color='skyblue')
axs[0].set_xlabel('Age Groups')
axs[0].set_ylabel('Population')
axs[0].set_title('Population Distribution by Age Groups')

# Gender populations
genders = ['Male', 'Female']
gender_counts = [male_population, female_population]

# Pie chart for gender distribution
axs[1].pie(gender_counts, labels=genders, autopct='%1.1f%%', startangle=140, colors=['lightblue', 'pink'])
axs[1].set_title('Gender Distribution')

# Main title
plt.suptitle(f'{name} (Total Population: {total_population})', fontsize=16)

# Adjust layout
plt.tight_layout(rect=[0, 0, 1, 0.95])
plt.show()
