# Data_Analysis_Election

Project Overview
This project is designed to practice data handling and analysis, focusing on various socio-economic factors and their potential relationships with election results across the United States. The dataset includes information on unemployment, population statistics, urbanization, and election results from all states spanning from 2000 to 2019.

Objectives
Data Collection and Importing: Gather data from various sources and import it for analysis.
Data Verification: Ensure the data is consistent and accurate.
Data Visualization: Create visual representations of the data using various types of charts, including a shapefile map of the United States.
Data Analysis: Analyze the data to identify potential relationships between socio-economic factors and election results, particularly presidential election outcomes.
Data Collection
The dataset includes:

Unemployment rates
Population statistics
Urbanization levels
Election results (Presidential elections)
Data was collected from multiple reliable sources, ensuring a comprehensive dataset for analysis.

Data Importing
The data was imported using Python scripts. Key steps included:

Loading data from CSV files
Preprocessing data to handle missing values and inconsistencies
Data Verification
To ensure the accuracy of the data, several checks were performed:

Consistency checks to verify that data values make sense
Cross-referencing with known data points to detect and correct discrepancies
Data Visualization
Various types of visualizations were created to understand the data better:

Unemployment Rates

Population Statistics

Urbanization Levels

Election Results

Analysis and Results
The main goal of the analysis was to determine if there is a relationship between the socio-economic data of each state and its presidential election results. Several techniques were employed:

K-Nearest Neighbors (KNN)
Used to classify states based on their socio-economic data and predict election outcomes.

Linear Regression
Applied to identify trends and relationships between individual socio-economic factors and election results.

Polynomial Regression
Utilized to capture more complex relationships between the data variables.

Findings
The analysis revealed that the data has too much variance to indicate a clear relationship between the socio-economic factors and election outcomes. This suggests that a more direct study of individual voter characteristics would be necessary to obtain meaningful insights. Factors such as political preferences (Democrat or Republican) likely require detailed individual-level data to reduce variance and improve the accuracy of predictions.

Conclusion
This project provided valuable insights into the complexities of socio-economic data analysis and its relationship with election results. While no definitive conclusions were drawn, the process highlighted the importance of comprehensive data and the challenges of variance in large datasets.
