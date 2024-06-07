"""
This script reads population data from a database, processes it, and displays it using a GUI. 
It provides visualizations of population distribution by age and gender.
"""

import configparser
import pandas as pd
import matplotlib.pyplot as plt
from sqlalchemy import create_engine
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
from tkinter import Tk, StringVar, OptionMenu, Frame, BOTH, HORIZONTAL, IntVar, Scale, Label

# Function to read database configuration
def read_db_config(config_file='config.ini'):
    """
    Reads the database configuration from the specified config file.
    """
    config = configparser.ConfigParser()
    config.read(config_file)
    return config['postgresql']

# Function to create a database engine
def create_db_engine(db_params):
    """
    Creates a SQLAlchemy engine using the provided database parameters.
    """
    return create_engine(f"postgresql+psycopg2://{db_params['user']}:{db_params['password']}@{db_params['host']}:{db_params['port']}/{db_params['dbname']}")

# Function to fetch population data from the database
def fetch_population_data(engine):
    """
    Fetches population data from the database and returns it as a pandas DataFrame.
    """
    query = """
        SELECT name, year, total_population, population_0_4, population_5_17, population_18_24, 
               population_25_44, population_45_64, population_65_plus,
               male_population, female_population
        FROM population_data
    """
    return pd.read_sql(query, engine)

# Function to plot population data
def plot_population_data(df, state_name, year):
    """
    Plots the population data for a specific state and year.
    """
    result = df[(df['name'] == state_name) & (df['year'] == year)]

    if result.empty:
        raise ValueError(f"No data found for the year {year} and state {state_name}")

    result = result.iloc[0]

    name = result['name']
    total_population = result['total_population']
    population_0_4 = result['population_0_4']
    population_5_17 = result['population_5_17']
    population_18_24 = result['population_18_24']
    population_25_44 = result['population_25_44']
    population_45_64 = result['population_45_64']
    population_65_plus = result['population_65_plus']
    male_population = result['male_population']
    female_population = result['female_population']

    percentages = [
        population_0_4 / total_population * 100,
        population_5_17 / total_population * 100,
        population_18_24 / total_population * 100,
        population_25_44 / total_population * 100,
        population_45_64 / total_population * 100,
        population_65_plus / total_population * 100,
    ]

    male_percentage = male_population / total_population * 100
    female_percentage = female_population / total_population * 100
    gender_percentages = [male_percentage, female_percentage]

    fig, axs = plt.subplots(1, 2, figsize=(18, 8))

    age_groups = ['0-4', '5-17', '18-24', '25-44', '45-64', '65+']
    bars = axs[0].bar(age_groups, percentages, color='skyblue')
    axs[0].set_title('Population by Age Group (%)')
    axs[0].set_xlabel('Age Group')
    axs[0].set_ylabel('Percentage of Total Population')
    axs[0].set_ylim(0, 40)

    male_bar = axs[1].barh(['Gender'], [gender_percentages[0]], color='lightblue', label='Male')
    female_bar = axs[1].barh(['Gender'], [gender_percentages[1]], left=[gender_percentages[0]], color='pink', label='Female')
    axs[1].set_xlim(0, 100)
    axs[1].set_title('Gender Distribution')
    axs[1].legend()
    axs[1].set_yticklabels([])  # Remove the y-axis label

    male_text = axs[1].text(gender_percentages[0] / 2, 0, f'{gender_percentages[0]:.1f}%', va='center', ha='center', color='black')
    female_text = axs[1].text(gender_percentages[0] + gender_percentages[1] / 2, 0, f'{gender_percentages[1]:.1f}%', va='center', ha='center', color='black')

    fig.suptitle(f'{name} (Total Population: {total_population}) - Year {year}', fontsize=16)
    fig.tight_layout(rect=[0, 0, 1, 0.95])

    return fig, bars, (male_bar, female_bar, male_text, female_text), name, total_population, year

# Function to update the plot based on user input
def update_plot(*args):
    """
    Updates the plot based on user input from the dropdown and slider.
    """
    state_name = selected_state.get()
    year = selected_year.get()
    result = df[(df['name'] == state_name) & (df['year'] == year)]

    if result.empty:
        print(f"No data found for the year {year} and state {state_name}")
        return

    result = result.iloc[0]

    name = result['name']
    total_population = result['total_population']
    population_0_4 = result['population_0_4']
    population_5_17 = result['population_5_17']
    population_18_24 = result['population_18_24']
    population_25_44 = result['population_25_44']
    population_45_64 = result['population_45_64']
    population_65_plus = result['population_65_plus']
    male_population = result['male_population']
    female_population = result['female_population']

    percentages = [
        population_0_4 / total_population * 100,
        population_5_17 / total_population * 100,
        population_18_24 / total_population * 100,
        population_25_44 / total_population * 100,
        population_45_64 / total_population * 100,
        population_65_plus / total_population * 100,
    ]

    male_percentage = male_population / total_population * 100
    female_percentage = female_population / total_population * 100
    gender_percentages = [male_percentage, female_percentage]

    for bar, height in zip(bars, percentages):
        bar.set_height(height)

    male_bar, female_bar, male_text, female_text = gender_bars
    male_bar[0].set_width(gender_percentages[0])
    female_bar[0].set_width(gender_percentages[1])
    female_bar[0].set_x(gender_percentages[0])

    male_text.set_x(gender_percentages[0] / 2)
    male_text.set_text(f'{gender_percentages[0]:.1f}%')
    female_text.set_x(gender_percentages[0] + gender_percentages[1] / 2)
    female_text.set_text(f'{gender_percentages[1]:.1f}%')

    fig.suptitle(f'{name} (Total Population: {total_population}) - Year {year}', fontsize=16)

    canvas.draw()

# Function to handle GUI closing event
def on_closing():
    """
    Handles the GUI closing event.
    """
    root.quit()
    root.destroy()

# Main function to setup and run the GUI
def main():
    """
    Sets up and runs the GUI for displaying population data.
    """
    global selected_state, selected_year, df, bars, gender_bars, fig, canvas

    db_params = read_db_config()
    engine = create_db_engine(db_params)
    df = fetch_population_data(engine)

    state_names = sorted(df['name'].unique())

    global root
    root = Tk()
    root.title("Population Data Viewer")
    root.geometry("1200x800")
    root.protocol("WM_DELETE_WINDOW", on_closing)

    selected_state = StringVar(root)
    selected_state.set(state_names[0])
    selected_state.trace_add('write', update_plot)

    selected_year = IntVar(root)
    selected_year.set(2019)
    selected_year.trace_add('write', update_plot)

    controls_frame = Frame(root)
    controls_frame.pack()

    dropdown_state = OptionMenu(controls_frame, selected_state, *state_names)
    dropdown_state.grid(row=0, column=0, padx=5, pady=5)

    year_label = Label(controls_frame, text="Year:")
    year_label.grid(row=0, column=1, padx=5, pady=5)

    year_slider = Scale(controls_frame, from_=2000, to=2019, orient=HORIZONTAL, variable=selected_year)
    year_slider.grid(row=0, column=2, padx=5, pady=5)

    plot_frame = Frame(root)
    plot_frame.pack(fill=BOTH, expand=True)

    # Remove the initial plot
    fig, bars, gender_bars, name, total_population, year = plot_population_data(df, selected_state.get(), selected_year.get())

    canvas = FigureCanvasTkAgg(fig, master=plot_frame)
    canvas.draw()
    canvas.get_tk_widget().pack(fill=BOTH, expand=True)

    root.mainloop()

if __name__ == "__main__":
    main()
