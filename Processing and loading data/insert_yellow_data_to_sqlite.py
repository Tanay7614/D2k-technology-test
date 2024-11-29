import sqlite3
import pandas as pd
import os

# Function to clean and transform Yellow Taxi data
def clean_and_transform_yellow_data(df):
    # Convert datetime columns to datetime format
    df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'], errors='coerce')
    df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'], errors='coerce')

    # Remove rows with missing pickup or dropoff datetimes
    df = df.dropna(subset=['tpep_pickup_datetime', 'tpep_dropoff_datetime'])

    # Calculate trip_duration in hours
    df['trip_duration'] = (df['tpep_dropoff_datetime'] - df['tpep_pickup_datetime']).dt.total_seconds() / 3600  # in hours

    # Calculate average speed in miles per hour (avg_speed = trip_distance / trip_duration)
    df['avg_speed'] = df['trip_distance'] / df['trip_duration']

    # Aggregate data: total trips and average fare per day
    df['date'] = df['tpep_pickup_datetime'].dt.date
    daily_aggregates = df.groupby('date').agg(
            total_trips=pd.NamedAgg(column='tpep_pickup_datetime', aggfunc='count'),
            avg_fare=pd.NamedAgg(column='fare_amount', aggfunc='mean')
        ).reset_index()
    print(daily_aggregates.head(10))

    # Keep only the necessary columns for insertion into SQLite
    df = df[['tpep_pickup_datetime', 'tpep_dropoff_datetime', 'PULocationID', 'DOLocationID', 'trip_distance', 'fare_amount', 'passenger_count', 'trip_duration', 'avg_speed']]

    return df

# Function to insert data into SQLite
def insert_into_sqlite(df, sqlite_db):
    # Create a connection to SQLite database
    conn = sqlite3.connect(sqlite_db)
    cursor = conn.cursor()

    # Create the table if it doesn't exist already
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS yellow_taxi_data (
        tpep_pickup_datetime DATETIME NOT NULL,
        tpep_dropoff_datetime DATETIME NOT NULL,
        PULocationID INTEGER,
        DOLocationID INTEGER,
        trip_distance REAL,
        fare_amount REAL,
        passenger_count INTEGER,
        trip_duration REAL,
        avg_speed REAL
    );
    ''')

    # Insert the data into the table
    df.to_sql('yellow_taxi_data', conn, if_exists='append', index=False, chunksize=1000)

    # Commit the transaction and close the connection
    conn.commit()
    conn.close()

# Folder path containing the Yellow Taxi data file(s)
input_folder = r"C:\Users\Minfy\Desktop\Assignment-d2k-tech\data"

# Loop through all files in the data folder
for file_name in os.listdir(input_folder):
    if file_name.endswith(".parquet") and "yellow" in file_name:
        file_path = os.path.join(input_folder, file_name)

        # Read the Yellow Taxi data
        df = pd.read_parquet(file_path)

        # Process the data
        processed_data = clean_and_transform_yellow_data(df)

        # Insert the processed data into the SQLite database
        insert_into_sqlite(processed_data, r"C:\Users\Minfy\Desktop\Assignment-d2k-tech\taxi_data.db")

        print(f"Data from {file_name} inserted into SQLite successfully.")

print("Data processing and insertion completed.")
