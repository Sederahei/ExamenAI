import requests
import json
import os
from datetime import datetime, timedelta

def extract_data(start_date_str: str, end_date_str: str): # Ajout des paramètres de date
    data_dir = "data"
    if not os.path.exists(data_dir):
        os.makedirs(data_dir)
        print(f"Répertoire '{data_dir}' créé.")
    else:
        print(f"Répertoire '{data_dir}' existe déjà.")

    start_date = datetime.strptime(start_date_str, '%Y-%m-%d').date()
    end_date = datetime.strptime(end_date_str, '%Y-%m-%d').date()
    today = datetime.now().date() # Date actuelle pour séparer historique/prévision

    cities = {
        'paris': {'lat': 48.85, 'lon': 2.35},
        'new_york': {'lat': 40.71, 'lon': -74.00},
        'tokyo': {'lat': 35.68, 'lon': 139.69},
        'cairo': {'lat': 30.04, 'lon': 31.23},
        'mahajanga-madagascar': {'lat': -18.91, 'lon': 46.31}
    }

    hourly_params = "temperature_2m,precipitation,weather_code,wind_speed_10m,cloud_cover,relative_humidity_2m"

    for city, coords in cities.items():
        all_city_data = {} # Pour stocker les données combinées historique + prévision

        # --- Partie Historique ---
        # Si la période demandée inclut le passé (jusqu'à hier)
        if start_date < today:
            historical_end_date = min(end_date, today - timedelta(days=1)) # Historique jusqu'à hier max
            if start_date <= historical_end_date:
                print(f"Récupération des données historiques pour {city} du {start_date} au {historical_end_date}...")
                historical_url = f"https://api.open-meteo.com/v1/archive?latitude={coords['lat']}&longitude={coords['lon']}&start_date={start_date}&end_date={historical_end_date}&hourly={hourly_params}&timezone=auto"
                historical_response = requests.get(historical_url)
                if historical_response.status_code == 200:
                    historical_data = historical_response.json()
                    # Open-Meteo renvoie un dictionnaire avec 'hourly' dedans. Nous voulons combiner les données horaires.
                    if 'hourly' in historical_data:
                        all_city_data['hourly'] = historical_data['hourly']
                    else:
                        print(f"ATTENTION: Pas de données horaires historiques pour {city} pour la période {start_date} à {historical_end_date}.")
                else:
                    print(f"Erreur API Historique pour {city} (Code: {historical_response.status_code}): {historical_response.text}")
        
        # --- Partie Prévision ---
        # Si la période demandée inclut le futur (à partir d'aujourd'hui)
        forecast_start_date = max(start_date, today) # Prévision à partir d'aujourd'hui au plus tôt
        if forecast_start_date <= end_date:
            print(f"Récupération des prévisions pour {city} du {forecast_start_date} au {end_date}...")
            # L'API forecast n'a pas de paramètre end_date, elle donne N jours à partir d'aujourd'hui.
            # Nous devons la récupérer et filtrer ensuite dans le transform.
            # Pour l'instant, on se base sur la période maximale de la forecast (généralement 16 jours)
            forecast_url = f"https://api.open-meteo.com/v1/forecast?latitude={coords['lat']}&longitude={coords['lon']}&hourly={hourly_params}&timezone=auto"
            forecast_response = requests.get(forecast_url)
            if forecast_response.status_code == 200:
                forecast_data = forecast_response.json()
                # Si nous avons des données historiques, nous combinons ; sinon, nous prenons les prévisions directement.
                if 'hourly' in all_city_data and 'hourly' in forecast_data:
                    # Combiner les données horaires : prendre les clés et étendre les listes
                    for key in forecast_data['hourly']:
                        if key in all_city_data['hourly']:
                            # Éviter les doublons si 'today' est inclus dans les deux (API archive finit hier, forecast commence aujourd'hui)
                            # On concatène simplement, le DataFrame gérera les doublons de date/heure s'il y en a.
                            all_city_data['hourly'][key].extend(forecast_data['hourly'][key])
                        else:
                            all_city_data['hourly'][key] = forecast_data['hourly'][key]
                elif 'hourly' in forecast_data:
                    all_city_data['hourly'] = forecast_data['hourly']
                else:
                    print(f"ATTENTION: Pas de données horaires de prévisions pour {city}.")
            else:
                print(f"Erreur API Prévision pour {city} (Code: {forecast_response.status_code}): {forecast_response.text}")

        # Enregistrement des données combinées
        if 'hourly' in all_city_data and all_city_data['hourly'].get('time'): # Vérifie qu'il y a des données horaires
            file_path = os.path.join(data_dir, f"{city}_raw.json")
            with open(file_path, "w") as f:
                json.dump(all_city_data, f) # Enregistre toutes les données combinées
            print(f"Données combinées pour {city} enregistrées dans '{file_path}'.")
        else:
            print(f"Aucune donnée valide à enregistrer pour {city} pour la période {start_date_str} au {end_date_str}.")