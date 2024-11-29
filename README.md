# NYC Taxi Data Analysis

## Project Overview
This project focuses on analyzing New York City taxi data to gain insights into passenger trends, peak hours, and fare statistics for different taxi types, including yellow taxis, green taxis, FHV (For-Hire Vehicles), and FHVHV (For-Hire High-Volume Vehicles). 

The objective of this project is to:
- Identify peak operating hours for different taxi types.
- Analyze passenger count and fare trends.
- Provide data visualizations for better understanding and decision-making.

## Environment Setup
To set up the development environment, follow the steps below:

### Required Software Installations
1. **Python**: Ensure you have Python 3.8 or above installed. [Download Python here](https://www.python.org/downloads/).
2. **Git**: Install Git to clone the repository and manage version control. [Download Git here](https://git-scm.com/).
3. **Sqlite database** : Install sqlite database.
   ```
4. **Dependencies**: Install the packages pandas, seaborn, matplotlib.
   ```

### Clone the Repository
Use the following command to clone the GitHub repository:
```bash
git clone https://github.com/your-username/nyc-taxi-analysis.git
```

### Directory Structure
Ensure your project folder is organized as follows:
```
Analysis and visualization of data/
  ├── Chart of visualization
     |-- pic of data               # Folder for raw and processed data
  ├── Yellow_taxi_data_visualize.py
  |--fhv_taxi_data_visualize.py
  |--fhvhv_taxi_data_visualize.py
  |--green_taxi_data_visualize.py
Code_to_fetch_data_2019/
  |-fetch_taxi_data_2019.py
Processing and loading data/
  |--insert_fhv_data_to_sqlite.py
  |--insert_fhvhv_data_to_sqlite.py
  |--insert_green_data_to_sqlite.py
  |--insert_yellow_data_to_sqlite.py
   

## Running the Project

Follow these steps to run the project:

### Step 1: Data Preparation
1. Download the NYC taxi data from the appropriate data source (e.g., NYC Taxi and Limousine Commission datasets).
2. Place the data files into the `data/` folder.

### Step 2: Data Processing
Run the provided scripts to preprocess and clean the data:
```bash
python scripts/data_processing.py
```
This will:
- Remove missing or invalid data entries.
- Format the data into a usable structure.

### Step 3: Generate Visualizations
Execute the visualization scripts to create graphs and charts:
```bash
python scripts/generate_visualizations.py
```
This will generate the following images and store them in the `visualizations/` folder:
- **passenger_count_yellow_taxi.png**: Passenger count vs average fare for yellow taxis.
- **peak_hour_green_taxi.png**: Peak hours for green taxis.
- **peak_hours_fhv_taxi.png**: Peak hours for For-Hire Vehicles.
- **peak_hours_fhvhv_taxi.png**: Peak hours for High-Volume For-Hire Vehicles.

### Step 4: Viewing Visualizations
Open the generated visualization images from the `visualizations/` folder to analyze the data.

## Data Analysis

### Querying the Data
Use Python scripts or SQL queries (if stored in a database) to extract insights. Example SQL query:
```sql
SELECT pickup_hour, COUNT(*) AS trip_count
FROM taxi_trips
WHERE taxi_type = 'green'
GROUP BY pickup_hour
ORDER BY trip_count DESC;
```

This query helps identify the busiest hours for green taxis.

### Visualizing the Data
The project includes visualizations like:
1. **Passenger Count Trends**:
   - Analyzed the relationship between passenger count and fare amount.
   - Example graph: `passenger_count_yellow_taxi.png`.
2. **Peak Hour Analysis**:
   - Identified the most frequent ride hours for different taxi types.
   - Example graphs: `peak_hour_green_taxi.png`, `peak_hours_fhv_taxi.png`, `peak_hours_fhvhv_taxi.png`.

These visualizations provide actionable insights for stakeholders to optimize services.
