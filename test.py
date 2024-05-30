import os
import pandas as pd
from datetime import datetime, timedelta
import numpy as np
# Define the base directory
base_dir = 'dataset'

# Create the base directory if it doesn't exist
os.makedirs(base_dir, exist_ok=True)

# Define the start date and time
start_date = datetime(2024, 5, 2, 9, 30)

# Generate and save files
for i in range(50):
    # Create a folder for each date
    folder_name = start_date.strftime('%Y%m%d')
    folder_path = os.path.join(base_dir, folder_name)
    os.makedirs(folder_path, exist_ok=True)
    
    # Generate and save 40 CSV files for each time
    for j in range(40):
        # Generate dummy data
        data = {
        'Strike Price': np.random.randint(100, 150, 50),
        'CALL_LTP': np.random.uniform(5.0, 10.0, 50),
        'PUT_LTP': np.random.uniform(4.0, 9.0, 50),
        'GAMMA_CALL': np.random.uniform(0.1, 0.3, 50)
         }
        
        # Create DataFrame
        df = pd.DataFrame(data)
        
        # Generate the timestamp for the file name
        timestamp = (start_date + timedelta(minutes=j)).strftime('%H%M')
        
        # Define the file path
        file_name = f'{timestamp}.csv'
        file_path = os.path.join(folder_path, file_name)
        
        # Save DataFrame to CSV file
        df.to_csv(file_path, index=False)
    start_date = start_date + timedelta(days=1)
    start_date = datetime(start_date.year, start_date.month, start_date.day,hour= 9, minute=30)