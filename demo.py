import os
import pandas as pd

# Function to clean and transform data
def clean_and_transform_data(df, taxi_type):
    # Map dataset-specific pickup and dropoff datetime columns
    datetime_columns = {
        'green': ('lpep_pickup_datetime', 'lpep_dropoff_datetime'),
        'yellow': ('tpep_pickup_datetime', 'tpep_dropoff_datetime'),
        'fhv': ('pickup_datetime', 'dropOff_datetime'),
        'fhvhv': ('pickup_datetime', 'dropoff_datetime')
    }

    # Get correct column names for the dataset
    pickup_col, dropoff_col = datetime_columns[taxi_type]

    # Convert pickup and dropoff time to datetime
    df[pickup_col] = pd.to_datetime(df[pickup_col], errors='coerce')
    df[dropoff_col] = pd.to_datetime(df[dropoff_col], errors='coerce')

    # Remove rows with missing or corrupt data
    required_columns = [pickup_col, dropoff_col]
    if 'fare_amount' in df.columns:
    # Add 'fare_amount' to required_columns and drop rows with NaN in these columns
        required_columns.append('fare_amount')
    df = df.dropna(subset=required_columns)
    

    # Derive new columns: trip duration and average speed
    df['trip_duration'] = (df[dropoff_col] - df[pickup_col]).dt.total_seconds() / 60  # in minutes
    if 'trip_distance' in df.columns:
        df['avg_speed'] = df['trip_distance'] / (df['trip_duration'] / 60)  # distance / hours = speed in mph
    else:
        df['avg_speed'] = None  # Set as None if 'trip_distance' is not available
    new_df=df
    # Aggregate data: total trips and average fare per day
    df['date'] = df[pickup_col].dt.date
    if 'fare_amount' in df.columns:
        daily_aggregates = df.groupby('date').agg(
            total_trips=pd.NamedAgg(column=pickup_col, aggfunc='count'),
            avg_fare=pd.NamedAgg(column='fare_amount', aggfunc='mean')
        ).reset_index()
    else:
        daily_aggregates = df.groupby('date').agg(
            total_trips=pd.NamedAgg(column=pickup_col, aggfunc='count')
        ).reset_index()
        daily_aggregates['avg_fare'] = None
    # Add a taxi_type column for identification
    daily_aggregates['taxi_type'] = taxi_type
    return new_df

# Function to process files and store the data in separate files based on taxi type
def process_and_store_separate_files(input_folder):
    # Define output file paths
    yellow_output_file = r"C:\Users\Minfy\Desktop\Assignment-d2k-tech\process_data\processed_yellow_taxi.csv"
    green_output_file = r"C:\Users\Minfy\Desktop\Assignment-d2k-tech\process_data\processed_green_taxi.csv"
    fhv_output_file = r"C:\Users\Minfy\Desktop\Assignment-d2k-tech\process_data\processed_fhv_taxi.csv"
    fhvhv_output_file = r"C:\Users\Minfy\Desktop\Assignment-d2k-tech\processed_fhv_hv_taxi.csv"

    # Loop through all files in the data folder
    for file_name in os.listdir(input_folder):
        if file_name.endswith(".parquet"):
            file_path = os.path.join(input_folder, file_name)

            # Determine taxi type based on file name
            if "green" in file_name:
                taxi_type = 'green'
            elif "yellow" in file_name:
                taxi_type = 'yellow'
            elif "fhv" in file_name and "fhvhv" not in file_name:
                taxi_type = 'fhv'
            elif "fhvhv" in file_name:
                taxi_type = 'fhvhv'
            else:
                print(f"Skipping unrecognized file: {file_name}")
                continue

            # Read file and process data
            df = pd.read_parquet(file_path)
            processed_data = clean_and_transform_data(df, taxi_type)

            # Determine output file based on taxi type
            if taxi_type == 'green':
                output_file = green_output_file
            elif taxi_type == 'yellow':
                output_file = yellow_output_file
            elif taxi_type == 'fhv':
                output_file = fhv_output_file
            elif taxi_type == 'fhvhv':
                output_file = fhvhv_output_file

            # Append processed data to the respective file
            processed_data.to_csv(output_file, mode='a', header=not os.path.exists(output_file), index=False)
            print(processed_data.head(10))
            print(processed_data.dtypes)
            break

# Define the folder path containing the files
input_folder = r"C:\Users\Minfy\Desktop\Assignment-d2k-tech\data"

# Process all files and store them in separate files
process_and_store_separate_files(input_folder)

print("Data processing completed and stored in separate files.")
