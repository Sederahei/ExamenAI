import requests
import json
from datetime import datetime

def extract_data():
    cities = {
        'paris': {'lat': 48.85, 'lon': 2.35},
        'new_york': {'lat': 40.71, 'lon': -74.00},
        'tokyo': {'lat': 35.68, 'lon': 139.69},
        'cairo': {'lat': 30.04, 'lon': 31.23},
        'mahajanga-madagascar': {'lat': -18.91, 'lon': 46.31}
    }

    for city, coords in cities.items():
        url = f"https://api.open-meteo.com/v1/forecast?latitude={coords['lat']}&longitude={coords['lon']}&hourly=temperature_2m,precipitation"

        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()
            with open(f"data/{city}_raw.json", "w") as f:
                json.dump(data, f)
        else:
            print(f"Erreur API pour {city} , veillez verifier les coordonnées, ou votre API")
