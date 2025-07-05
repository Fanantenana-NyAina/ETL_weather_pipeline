import pandas as pd

def merge_all() -> str:
    """
    Fusion des données recents et les historiques
    """
    
    historical_file = "/home/fanantenana/airflow/dags/weather_difference_pipeline/data/processed/historical_weather.csv"
    realtime_file = "data/processed/recent_global_weather.csv"
    output_file = "data/processed/meteo_global_all_in_one.csv"

    # Load historical data
    hist_df = pd.read_csv(historical_file)

    # Load recent real-time data
    real_df = pd.read_csv(realtime_file)

    # Combine both datasets
    merged_df = pd.concat([hist_df, real_df], ignore_index=True)

    # Remove duplicate rows (keep the latest record per city & date)
    merged_df = merged_df.drop_duplicates(
        subset=['ville', 'date_extraction'],
        keep='last'
    )

    # Save merged data to CSV (local dir)
    merged_df.to_csv(output_file, index=False)

    return output_file
