import requests
import json
from datetime import datetime
import os # Importez le module os

def extract_data():
    # Définir le chemin du répertoire de données
    data_dir = "data"
    # Créer le répertoire s'il n'existe pas
    if not os.path.exists(data_dir):
        os.makedirs(data_dir)

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
            # Utiliser os.path.join pour une construction de chemin robuste
            file_path = os.path.join(data_dir, f"{city}_raw.json")
            with open(file_path, "w") as f:
                json.dump(data, f)
        else:
            print(f"Erreur API pour {city}")