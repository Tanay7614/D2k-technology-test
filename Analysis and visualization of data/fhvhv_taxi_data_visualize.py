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
    strftime('%H', pickup_datetime) AS hour,
    COUNT(*) AS trip_count
FROM 
    fhvhv_trip_data
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
plt.title("Peak Hours for Taxi Usage (FHVHV Taxi)", fontsize=16)
plt.xlabel("Hour of Day (24-hour format)", fontsize=12)
plt.ylabel("Number of Trips", fontsize=12)
plt.xticks(fontsize=10)
plt.yticks(fontsize=10)
plt.grid(axis='y', linestyle='--', alpha=0.7)
plt.tight_layout()
plt.show()

# Query 2: Trends in Monthly Trip Counts
query2 = """
SELECT 
    strftime('%Y-%m', pickup_datetime) AS month, 
    COUNT(*) AS trip_count
FROM 
    fhvhv_trip_data
GROUP BY 
    month
ORDER BY 
    month;
"""

# Execute Query 2
df_trend = pd.read_sql_query(query2, conn)

# Visualization 2: Monthly Usage Trends
plt.figure(figsize=(12, 6))
plt.plot(df_trend['month'], df_trend['trip_count'], marker='o', linestyle='-', color='orange', label='Monthly Trips')
plt.title("Monthly Usage Trends (FHVHV Taxi)", fontsize=16)
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
