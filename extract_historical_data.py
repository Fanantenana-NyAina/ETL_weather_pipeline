import os
import pandas as pd

CITIES = ['Vancouver', 'San Francisco', 'Toronto', 'Jerusalem', 'Los Angeles', 'Montreal', 'Nashville']

def extract_historical_meteo() -> str:
    """
    Récupération et fusion des 7 fichiers qui contiennent les données historiques des villes cibles
    """
    base_dir = "/home/fanantenana/airflow/dags/weather_difference_pipeline/data"
    output_file = "/home/fanantenana/airflow/dags/weather_difference_pipeline/data/processed/historical_weather.csv"

    city_attrs = pd.read_csv(os.path.join(base_dir, "city_attributes.csv"))

    temp_df = pd.read_csv(os.path.join(base_dir, "temperature.csv"))
    humidity_df = pd.read_csv(os.path.join(base_dir, "humidity.csv"))
    pressure_df = pd.read_csv(os.path.join(base_dir, "pressure.csv"))
    wind_speed_df = pd.read_csv(os.path.join(base_dir, "wind_speed.csv"))
    wind_dir_df = pd.read_csv(os.path.join(base_dir, "wind_direction.csv"))
    description_df = pd.read_csv(os.path.join(base_dir, "weather_description.csv"))

    merged_df = temp_df.melt(id_vars=["datetime"], var_name="ville", value_name="temperature")

    def melt_and_merge(df, varname):
        return df.melt(id_vars=["datetime"], var_name="ville", value_name=varname)

    merged_df = merged_df.merge(melt_and_merge(humidity_df, "humidite"), on=["datetime", "ville"], how="left")
    merged_df = merged_df.merge(melt_and_merge(pressure_df, "pression"), on=["datetime", "ville"], how="left")
    merged_df = merged_df.merge(melt_and_merge(wind_speed_df, "wind_speed"), on=["datetime", "ville"], how="left")
    merged_df = merged_df.merge(melt_and_merge(wind_dir_df, "wind_direction"), on=["datetime", "ville"], how="left")
    merged_df = merged_df.merge(melt_and_merge(description_df, "description"), on=["datetime", "ville"], how="left")

    merged_df = merged_df.merge(city_attrs.rename(columns={"City": "ville"}), on="ville", how="left")
    merged_df = merged_df[merged_df["ville"].isin(CITIES)]

    merged_df = merged_df.rename(columns={"datetime": "date_extraction"})
    merged_df["date_extraction"] = pd.to_datetime(merged_df["date_extraction"])
    merged_df = merged_df[merged_df["date_extraction"].dt.hour == 12]
    
    merged_df["temperature"] = round(merged_df["temperature"] - 273.15, 2)

    merged_df = merged_df.dropna()

    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    merged_df.to_csv(output_file, index=False)

    return output_file
