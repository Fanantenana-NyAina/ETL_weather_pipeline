import os
import pandas as pd
import requests
import logging
from datetime import datetime

def extract_realtime_meteo(api_key: str, city: str, date: str) -> bool:
    """
    Extrait les données météo OpenWeather en temps réel.
    """
    try:
        url = "https://api.openweathermap.org/data/2.5/weather"
        params = {
            'q': city,
            'appid': api_key,
            'units': 'metric',
            'lang': 'fr'
        }
        
        res = requests.get(url, params=params, timeout=10)
        res.raise_for_status()

        data = res.json()
        realtime_weather_data = {
            'ville': city,
            'date_extraction': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            'temperature': data['main']['temp'],
            'humidite': data['main']['humidity'],
            'pression': data['main']['pressure'],
            'description': data['weather'][0]['description'],
            'wind_speed': data['wind']['speed'],
            'wind_direction': data['wind']['deg'],
            'Country': data['sys'].get('country'),
            'Latitude': data['coord']['lat'],
            'Longitude': data['coord']['lon']
        }

        os.makedirs(f"data/raw/{date}", exist_ok=True)
        pd.DataFrame([realtime_weather_data]).to_csv(
            f"data/raw/{date}/realtime_weather_{city}.csv",
            index=False
        )
        
        logging.info(f"Données récupérées pour {city}")
        return True

    except Exception as e:
        logging.error(f"Erreur d'extraction {city}: {e}")
        return False
