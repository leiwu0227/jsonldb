"""
Time series data handling with FolderDB.
Shows how to store and query time series data using datetime keys.
"""

import os
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from folderdb import FolderDB

def generate_sensor_data(start_time: datetime, num_points: int) -> pd.DataFrame:
    """Generate sample sensor data"""
    times = [start_time + timedelta(minutes=i) for i in range(num_points)]
    return pd.DataFrame({
        'temperature': np.random.normal(25, 2, num_points),
        'humidity': np.random.normal(60, 5, num_points),
        'pressure': np.random.normal(1013, 5, num_points)
    }, index=times)

def main():
    # Create a folder for our database
    db_folder = "timeseries_db"
    os.makedirs(db_folder, exist_ok=True)
    
    # Initialize the database
    db = FolderDB(db_folder)
    
    # Generate data for multiple sensors
    start_time = datetime(2024, 1, 1, 12, 0)
    sensor1_data = generate_sensor_data(start_time, 60)  # 1 hour of data
    sensor2_data = generate_sensor_data(start_time, 60)
    
    # Save data to database
    db.upsert_dfs({
        "sensor1": sensor1_data,
        "sensor2": sensor2_data
    })
    
    # Query last 30 minutes of data
    end_time = start_time + timedelta(minutes=59)
    start_time_query = end_time - timedelta(minutes=30)
    
    recent_data = db.get_df(
        ["sensor1", "sensor2"],
        lower_key=start_time_query,
        upper_key=end_time
    )
    
    # Calculate statistics
    for sensor in ["sensor1", "sensor2"]:
        data = recent_data[sensor]
        print(f"\n{sensor} statistics (last 30 minutes):")
        print(f"Temperature: {data['temperature'].mean():.1f}°C ± {data['temperature'].std():.1f}°C")
        print(f"Humidity: {data['humidity'].mean():.1f}% ± {data['humidity'].std():.1f}%")
        print(f"Pressure: {data['pressure'].mean():.1f}hPa ± {data['pressure'].std():.1f}hPa")
    
    # Apply calibration to sensor1
    calibration_df = pd.DataFrame({
        'temperature': [1.0],  # Add 1.0°C
        'humidity': [2.0],     # Add 2.0%
        'pressure': [0.0]      # No change
    }, index=[start_time])
    
    db.upsert_df("sensor1_calibration", calibration_df)
    
    # Delete old data (first 15 minutes)
    db.delete_range(
        ["sensor1", "sensor2"],
        lower_key=start_time,
        upper_key=start_time + timedelta(minutes=15)
    )
    
    # Show final state
    print("\nFinal database state:")
    print(str(db))
    
    # Cleanup
    for file in os.listdir(db_folder):
        os.remove(os.path.join(db_folder, file))
    os.rmdir(db_folder)

if __name__ == "__main__":
    main() 