import sqlite3
import pandas as pd
import os

# Function to clean and transform FHV data
def clean_and_transform_fhv_data(df):
    # Convert datetime columns to datetime format
    datetime_columns = ['pickup_datetime', 'dropOff_datetime']
    for col in datetime_columns:
        df[col] = pd.to_datetime(df[col], errors='coerce')
    
    # Drop rows with missing datetime values
    df = df.dropna(subset=['pickup_datetime', 'dropOff_datetime'])
    
    # Calculate trip duration in hours
    df['trip_duration'] = (df['dropOff_datetime'] - df['pickup_datetime']).dt.total_seconds() / 3600  # in hours

    # Calculate average speed in miles per hour (assuming trip distance is included elsewhere if needed)
    # Here, assuming you don't have 'trip_miles' available based on the columns you listed

     # Aggregate data: total trips
    df['date'] = df['pickup_datetime'].dt.date
    daily_aggregates = df.groupby('date').agg(
            total_trips=pd.NamedAgg(column='pickup_datetime', aggfunc='count')
        ).reset_index()
    print(daily_aggregates.head(10))

    # Keep only relevant columns for insertion into SQLite
    df = df[['dispatching_base_num', 'pickup_datetime', 'dropOff_datetime', 'PUlocationID', 'DOlocationID', 'SR_Flag', 'Affiliated_base_number', 'trip_duration']]
    
    return df

# Function to insert data into SQLite
def insert_into_sqlite(df, sqlite_db):
    # Create a connection to SQLite database
    conn = sqlite3.connect(sqlite_db)
    cursor = conn.cursor()

    # Drop the existing table if it exists
    cursor.execute('DROP TABLE IF EXISTS fhv_trip_data')

    # Create the table with the correct schema
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS fhv_trip_data (
        dispatching_base_num TEXT,
        pickup_datetime DATETIME NOT NULL,
        dropOff_datetime DATETIME NOT NULL,
        PUlocationID INTEGER,
        DOlocationID INTEGER,
        SR_Flag TEXT,
        Affiliated_base_number TEXT,
        trip_duration REAL
    );
    ''')

    # Insert the data into the table
    df.to_sql('fhv_trip_data', conn, if_exists='append', index=False, chunksize=1000)

    # Commit the transaction and close the connection
    conn.commit()
    conn.close()

# Folder path containing the FHV data file(s)
input_folder = r"C:\\Users\\Minfy\\Desktop\\Assignment-d2k-tech\\data"
sqlite_db_path = r"C:\\Users\\Minfy\\Desktop\\Assignment-d2k-tech\\taxi_data.db"

# Loop through all files in the data folder
for file_name in os.listdir(input_folder):
    if file_name.endswith(".parquet") and "fhv" in file_name and "fhvhv" not in file_name:
        file_path = os.path.join(input_folder, file_name)

        # Read the FHV data
        df = pd.read_parquet(file_path)

        # Debugging block
        print(f"Processing file: {file_name}")
        print(f"Columns in DataFrame: {df.columns.tolist()}")

        # Process the data
        processed_data = clean_and_transform_fhv_data(df)

        # Insert the processed data into the SQLite database
        insert_into_sqlite(processed_data, sqlite_db_path)

        print(f"Data from {file_name} inserted into SQLite successfully.")

print("Data processing and insertion completed.")
