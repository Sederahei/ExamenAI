import requests
import json
import os
from datetime import datetime

def extract_data():
    # Définir le chemin du répertoire de données
    data_dir = "data"
    # Créer le répertoire s'il n'existe pas
    if not os.path.exists(data_dir):
        os.makedirs(data_dir)
        print(f"Répertoire '{data_dir}' créé.")
    else:
        print(f"Répertoire '{data_dir}' existe déjà.")

    cities = {
        'paris': {'lat': 48.85, 'lon': 2.35},
        'new_york': {'lat': 40.71, 'lon': -74.00},
        'tokyo': {'lat': 35.68, 'lon': 139.69},
        'cairo': {'lat': 30.04, 'lon': 31.23},
        'mahajanga-madagascar': {'lat': -18.91, 'lon': 46.31} # Nom correct pour le fichier
    }

    # Paramètres supplémentaires pour une meilleure analyse de vacances
    # weather_code: Code WMO qui décrit la condition météorologique générale (ex: ensoleillé, nuageux, pluie, etc.)
    # wind_speed_10m: Vitesse du vent à 10 mètres au-dessus du sol
    # cloud_cover: Pourcentage de couverture nuageuse
    # relative_humidity_2m: Humidité relative à 2 mètres
    hourly_params = "temperature_2m,precipitation,weather_code,wind_speed_10m,cloud_cover,relative_humidity_2m"

    for city, coords in cities.items():
        url = f"https://api.open-meteo.com/v1/forecast?latitude={coords['lat']}&longitude={coords['lon']}&hourly={hourly_params}&timezone=auto"

        print(f"Tentative de récupération des données pour {city}...")
        response = requests.get(url)

        if response.status_code == 200:
            data = response.json()
            file_path = os.path.join(data_dir, f"{city}_raw.json")
            with open(file_path, "w") as f:
                json.dump(data, f)
            print(f"Données pour {city} enregistrées dans '{file_path}'.")
        else:
            print(f"Erreur API pour {city} (Code: {response.status_code}): {response.text}")
            # Si une ville échoue, nous n'arrêtons pas le processus, nous passons à la suivante.
            # La tâche de transformation devra gérer l'absence de certains fichiers.