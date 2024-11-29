import sqlite3
import pandas as pd
import os

# Function to clean and transform FHVHV Trip data
def clean_and_transform_fhvhv_data(df):
    # Convert datetime columns to datetime format
    df['pickup_datetime'] = pd.to_datetime(df['pickup_datetime'], errors='coerce')
    df['dropoff_datetime'] = pd.to_datetime(df['dropoff_datetime'], errors='coerce')

    # Remove rows with missing pickup or dropoff datetimes
    df = df.dropna(subset=['pickup_datetime', 'dropoff_datetime'])

    # Calculate trip_duration in hours
    df['trip_duration'] = (df['dropoff_datetime'] - df['pickup_datetime']).dt.total_seconds() / 3600  # in hours

    # Rename columns as necessary for consistency
    df['trip_distance'] = df['trip_miles']  # Renaming trip_miles to trip_distance for clarity

    # Calculate average speed in miles per hour (avg_speed = trip_distance / trip_duration)
    if 'trip_distance' in df.columns:
        df['avg_speed'] = df['trip_distance'] / df['trip_duration']
    else:
        df['trip_distance'] = None  # Fill with None for consistency
        df['avg_speed'] = None      # Fill with None since it can't be calculated

     # Aggregate data: total trips and average fare per day
    df['date'] = df['pickup_datetime'].dt.date
    daily_aggregates = df.groupby('date').agg(
            total_trips=pd.NamedAgg(column='pickup_datetime', aggfunc='count'),
            avg_fare=pd.NamedAgg(column='base_passenger_fare', aggfunc='mean')
        ).reset_index()
    print(daily_aggregates.head(10))

    # Keep only the necessary columns for insertion into SQLite
    df = df[['pickup_datetime', 'dropoff_datetime', 'PULocationID', 'DOLocationID', 'base_passenger_fare', 'trip_distance', 'trip_duration', 'avg_speed']]

    return df

# Function to insert data into SQLite
def insert_into_sqlite(df, sqlite_db):
    # Create a connection to SQLite database
    conn = sqlite3.connect(sqlite_db)
    cursor = conn.cursor()

    # Create the table if it doesn't exist already, with the correct schema
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS fhvhv_trip_data (
        pickup_datetime DATETIME NOT NULL,
        dropoff_datetime DATETIME NOT NULL,
        PULocationID INTEGER,
        DOLocationID INTEGER,
        base_passenger_fare REAL,
        trip_distance REAL,
        trip_duration REAL,
        avg_speed REAL
    );
    ''')

    # Insert the data into the table
    df.to_sql('fhvhv_trip_data', conn, if_exists='append', index=False, chunksize=1000)

    # Commit the transaction and close the connection
    conn.commit()
    conn.close()

# Folder path containing the FHVHV Trip data file(s)
input_folder = r"C:\\Users\\Minfy\\Desktop\\Assignment-d2k-tech\\data"
sqlite_db_path = r"C:\\Users\\Minfy\\Desktop\\Assignment-d2k-tech\\taxi_data.db"

# Loop through all files in the data folder
for file_name in os.listdir(input_folder):
    if file_name.endswith(".parquet") and "fhvhv" in file_name:
        file_path = os.path.join(input_folder, file_name)

        # Read the FHVHV Trip data
        df = pd.read_parquet(file_path)

        # Debugging block
        print(f"Processing file: {file_name}")
        # print(f"Columns in DataFrame: {df.columns.tolist()}")
        if 'trip_miles' not in df.columns:
            print(f"'trip_miles' column is missing in {file_name}")
        else:
            print(f"'trip_miles' column found with {df['trip_miles'].notna().sum()} non-null values")

        # Process the data
        processed_data = clean_and_transform_fhvhv_data(df)

        # Insert the processed data into the SQLite database
        insert_into_sqlite(processed_data, sqlite_db_path)

        print(f"Data from {file_name} inserted into SQLite successfully.")

print("Data processing and insertion completed.")
