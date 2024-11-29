import pandas as pd
import sqlite3
import os

# Define the SQLite database connection
conn = sqlite3.connect('taxi_data.db')
cursor = conn.cursor()

# Function to clean and transform green taxi data
def clean_and_transform_green_taxi_data(df):
    # Define the column names for green taxi
    pickup_col, dropoff_col = 'lpep_pickup_datetime', 'lpep_dropoff_datetime'
    
    # Convert pickup and dropoff time to datetime
    df[pickup_col] = pd.to_datetime(df[pickup_col], errors='coerce')
    df[dropoff_col] = pd.to_datetime(df[dropoff_col], errors='coerce')

    # Remove rows with missing or corrupt data
    df = df.dropna(subset=[pickup_col, dropoff_col, 'fare_amount'])

    # Derive new columns: trip duration in hours and avg speed
    df['trip_duration'] = (df[dropoff_col] - df[pickup_col]).dt.total_seconds() / 3600  # in hours
    df['avg_speed'] = df['trip_distance'] / df['trip_duration']  # in miles per hour

    # Ensure avg_speed is meaningful; replace infinity and NaN with 0
    df['avg_speed'] = df['avg_speed'].replace([float('inf'), -float('inf')], 0).fillna(0)

    # Aggregate data: total trips and average fare per day
    df['date'] = df['tpep_pickup_datetime'].dt.date
    daily_aggregates = df.groupby('date').agg(
            total_trips=pd.NamedAgg(column='tpep_pickup_datetime', aggfunc='count'),
            avg_fare=pd.NamedAgg(column='fare_amount', aggfunc='mean')
        ).reset_index()
    print(daily_aggregates.head(10))

    # Convert datetime columns to strings (ISO format)
    df[pickup_col] = df[pickup_col].dt.strftime('%Y-%m-%d %H:%M:%S')
    df[dropoff_col] = df[dropoff_col].dt.strftime('%Y-%m-%d %H:%M:%S')

    return df

# Function to insert the data into SQLite database
def insert_data_to_db(df):
    # Drop the table if it exists to avoid schema mismatch
    cursor.execute('DROP TABLE IF EXISTS green_taxi_data')

    # Create the table with the correct schema
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS green_taxi_data (
        lpep_pickup_datetime DATETIME NOT NULL,
        lpep_dropoff_datetime DATETIME NOT NULL,
        PULocationID INTEGER,
        DOLocationID INTEGER,
        passenger_count REAL,
        trip_distance REAL,
        trip_duration REAL,
        avg_speed REAL,
        fare_amount REAL
    )
    ''')

    # Insert the cleaned and transformed data into the database
    for _, row in df.iterrows():
        cursor.execute('''
        INSERT INTO green_taxi_data (
            lpep_pickup_datetime,
            lpep_dropoff_datetime,
            PULocationID,
            DOLocationID,
            passenger_count,
            trip_distance,
            trip_duration,
            avg_speed,
            fare_amount
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            row['lpep_pickup_datetime'],
            row['lpep_dropoff_datetime'],
            row['PULocationID'],
            row['DOLocationID'],
            row['passenger_count'],
            row['trip_distance'],
            row['trip_duration'],
            row['avg_speed'],
            row['fare_amount']
        ))
    conn.commit()

# Define the folder path containing the files
input_folder = r'C:\Users\Minfy\Desktop\Assignment-d2k-tech\data'

# Loop through all files in the data folder
for file_name in os.listdir(input_folder):
    if file_name.endswith(".parquet") and "green" in file_name.lower():  # Check if the file is a green taxi file
        file_path = os.path.join(input_folder, file_name)
        
        # Read the green taxi data
        green_taxi_data = pd.read_parquet(file_path)
        
        # Clean and transform the green taxi data
        processed_green_taxi_data = clean_and_transform_green_taxi_data(green_taxi_data)
        
        # Insert the processed data into SQLite database
        insert_data_to_db(processed_green_taxi_data)
        
        print(f"Processed and inserted green taxi data from {file_name} into the database.")

# Close the database connection
conn.close()

print("Green taxi data processing and insertion into the database is complete.")
