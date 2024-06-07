# Data Analysis Project

## Project Overview

This project is designed to practice data handling and analysis, focusing on various econimic and social factors and their potential relationships with election results across the United States. The dataset includes information on unemployment, GDP, population statistics, education, urbanization, and election results from all states spanning for many years.

## Objectives

1. **Data Collection and Importing**: Gather data from various sources and import it for analysis.
2. **Data Verification**: Ensure the data is consistent and accurate.
3. **Data Visualization**: Create visual representations of the data using various types of charts
4. **Data Analysis**: Analyze the data to identify potential relationships between econimic and social factors and election results, particularly presidential election outcomes.

## Data Collection

The dataset includes:
- Unemployment rates
- GDP Data
- College finisher rates
- Population statistics
- Urbanization levels
- Election results (Presidential elections from 2000-2020)

Data was collected from multiple sources, for a comprehensive dataset for analysis. But i do not garantee for the all this Data is correct and i did not check for correctness, since this was not the main goal of this project

## Data Importing

The data was imported using Python scripts. Key steps included:
- Loading data from CSV, Excel and Text files
- Preprocessing data to handle missing values and inconsistencies

## Data Verification

To ensure the accuracy of the data, several checks were performed:
- Consistency checks to verify that data values make sense
- Coverage to check, if there is Data missing for some years or states
- Check Datatypes and Format of the Data

## Data Visualization

Various types of visualizations were created to understand the data better:

### Unemployment Rates

![Unemployment Rates](Visualize_Data/Plots/unemployment_animation.gif)

### Population Statistics

![Population Statistics](Visualize_Data/Plots/gdp_per_person_by_state.gif)

### Age and Sex

![Urbanization Levels](images/urbanization_visualization.png)

### Election Results

![Election Results](Visualize_Data/Plots/Election_Map_2020.png)

## Analysis and Results

The main goal of the analysis was to determine if there is a relationship between the socio-economic data of each state and its presidential election results. Several techniques were employed:

### K-Nearest Neighbors (KNN)

Used to classify states based on their socio-economic data and predict election outcomes.

### Linear Regression

Applied to identify trends and relationships between individual socio-economic factors and election results.

### Polynomial Regression

Utilized to capture more complex relationships between the data variables.

### Findings

The analysis revealed that the data has too much variance to indicate a clear relationship between the socio-economic factors and election outcomes. This suggests that a more direct study of individual voter characteristics would be necessary to obtain meaningful insights. Factors such as political preferences (Democrat or Republican) likely require detailed individual-level data to reduce variance and improve the accuracy of predictions.

## Conclusion

This project provided valuable insights into the complexities of socio-economic data analysis and its relationship with election results. While no definitive conclusions were drawn, the process highlighted the importance of comprehensive data and the challenges of variance in large datasets.

## Code

The project includes several Python scripts for data processing, visualization, and analysis:

- `data_processing.py`: Handles data importing and preprocessing.
- `main.py`: Main script to run the analysis.
- `plot_functions.py`: Contains functions for generating various plots.
- `import_education.py`: Specific script for importing education data.
- `verify_age_sex_data.py`: Verifies and cleans age and sex data.
- `visualize_age_sex.py`: Generates visualizations for age and sex data.
- `visualize_election.py`: Creates visualizations for election results.
- `visualize_gdp.py`: Generates GDP visualizations.
- `visualize_unemployment.py`: Creates unemployment rate visualizations.
