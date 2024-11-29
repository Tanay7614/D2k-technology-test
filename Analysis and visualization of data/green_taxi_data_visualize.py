import sqlite3
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# SQLite database path
sqlite_db_path = r"C:\\Users\\Minfy\\Desktop\\Assignment-d2k-tech\\taxi_data.db"

# Connect to SQLite
conn = sqlite3.connect(sqlite_db_path)

# Query 1: Peak Hours for Taxi Usage
query1 = """
SELECT 
    strftime('%H', lpep_pickup_datetime) AS hour,
    COUNT(*) AS trip_count
FROM 
    green_taxi_data
GROUP BY 
    hour
ORDER BY 
    trip_count DESC;
"""

# Execute Query 1
df_peak_hours = pd.read_sql_query(query1, conn)

# Convert hour column to integer for proper sorting
df_peak_hours['hour'] = df_peak_hours['hour'].astype(int)
df_peak_hours = df_peak_hours.sort_values(by='hour')

# Visualization 1: Peak Hours
plt.figure(figsize=(10, 6))
sns.barplot(data=df_peak_hours, x='hour', y='trip_count', palette='viridis', legend=False)
plt.title("Peak Hours for Taxi Usage", fontsize=16)
plt.xlabel("Hour of Day (24-hour format)", fontsize=12)
plt.ylabel("Number of Trips", fontsize=12)
plt.xticks(fontsize=10)
plt.yticks(fontsize=10)
plt.grid(axis='y', linestyle='--', alpha=0.7)
plt.tight_layout()
plt.show()

# Query 2: Passenger Count vs. Average Fare
query2 = """
SELECT 
    passenger_count, 
    AVG(fare_amount) AS avg_fare, 
    COUNT(*) AS trip_count
FROM 
    green_taxi_data
GROUP BY 
    passenger_count
ORDER BY 
    passenger_count;
"""

# Execute Query 2
df_fare = pd.read_sql_query(query2, conn)

# Visualization 2: Passenger Count vs. Average Fare
plt.figure(figsize=(10, 6))
sns.barplot(data=df_fare, x='passenger_count', y='avg_fare', palette='coolwarm', legend=False)
plt.title("Passenger Count vs. Average Fare", fontsize=16)
plt.xlabel("Passenger Count", fontsize=12)
plt.ylabel("Average Fare ($)", fontsize=12)
plt.xticks(fontsize=10)
plt.yticks(fontsize=10)
plt.grid(axis='y', linestyle='--', alpha=0.7)
plt.tight_layout()
plt.show()

# Query 3: Monthly Trip Count Trends
query3 = """
SELECT 
    strftime('%Y-%m', lpep_pickup_datetime) AS month, 
    COUNT(*) AS trip_count
FROM 
    green_taxi_data
GROUP BY 
    month
ORDER BY 
    month;
"""

# Execute Query 3
df_trend = pd.read_sql_query(query3, conn)

# Visualization 3: Monthly Usage Trends
plt.figure(figsize=(12, 6))
plt.plot(df_trend['month'], df_trend['trip_count'], marker='o', linestyle='-', color='orange', label='Monthly Trips')
plt.title("Monthly Usage Trends", fontsize=16)
plt.xlabel("Month", fontsize=12)
plt.ylabel("Number of Trips", fontsize=12)
plt.xticks(rotation=45, fontsize=10)
plt.yticks(fontsize=10)
plt.grid(axis='y', linestyle='--', alpha=0.7)
plt.legend()
plt.tight_layout()
plt.show()

# Close the database connection
conn.close()
