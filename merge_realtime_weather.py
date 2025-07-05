import os
import pandas as pd

def merge_data_from_realtime(date: str) -> str:
    """
    Fusion des donnée meteo à temps réel des villes cibles.
    """
    input_dir = f"data/raw/{date}"
    output_file = "data/processed/recent_global_weather.csv"
    
    # Creating output directory if it does not exist
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    
    # Loading existing data if the file exists
    if os.path.exists(output_file):
        global_df = pd.read_csv(output_file)
    else:
        global_df = pd.DataFrame()
    
    # Loading new data files
    new_data = []
    for file in os.listdir(input_dir):
        if file.startswith('realtime_weather_') and file.endswith('.csv'):
            new_data.append(pd.read_csv(f"{input_dir}/{file}"))
    
    if not new_data:
        raise ValueError(f"No new data found to merge for {date}")
    
    # Combine old and new data
    updated_df = pd.concat([global_df] + new_data, ignore_index=True)
    
    # Keep only the most recent row per city
    updated_df = updated_df.drop_duplicates(
        subset=['ville'],
        keep='last'
    )
    
    # Save to CSV in local dir
    updated_df.to_csv(output_file, index=False)
    
    return output_file
