import os
import json
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

def transform_data(start_date_str: str, end_date_str: str):
    data_dir = "data"
    cities_coords = { # Utilisez cette structure pour la dim_city
        'paris': {'lat': 48.85, 'lon': 2.35, 'display_name': 'Paris', 'country': 'France', 'continent': 'Europe'},
        'new_york': {'lat': 40.71, 'lon': -74.00, 'display_name': 'New York', 'country': 'USA', 'continent': 'North America'},
        'tokyo': {'lat': 35.68, 'lon': 139.69, 'display_name': 'Tokyo', 'country': 'Japan', 'continent': 'Asia'},
        'cairo': {'lat': 30.04, 'lon': 31.23, 'display_name': 'Cairo', 'country': 'Egypt', 'continent': 'Africa'},
        'mahajanga-madagascar': {'lat': -18.91, 'lon': 46.31, 'display_name': 'Mahajanga', 'country': 'Madagascar', 'continent': 'Africa'}
    }
    cities = list(cities_coords.keys()) # Liste des clés pour itérer

    start_datetime = datetime.strptime(start_date_str, '%Y-%m-%d')
    end_datetime = datetime.strptime(end_date_str, '%Y-%m-%d') + timedelta(days=1, microseconds=-1)
    
    all_cities_hourly_dfs = [] # Liste pour le fichier de faits horaire

    # --- Génération de la Dimension Ville (dim_city) ---
    dim_city_data = []
    for city_key, details in cities_coords.items():
        dim_city_data.append({
            'city_key': city_key,
            'city_display_name': details['display_name'],
            'latitude': details['lat'],
            'longitude': details['lon'],
            'country': details['country'],
            'continent': details['continent']
        })
    dim_city_df = pd.DataFrame(dim_city_data)
    dim_city_df.to_csv(os.path.join(data_dir, "dim_city.csv"), index=False)
    print("Dimension ville (dim_city.csv) créée.")

    for city in cities:
        file_path = os.path.join(data_dir, f"{city}_raw.json")
        
        if not os.path.exists(file_path):
            print(f"ATTENTION: Le fichier brut pour '{city}' n'a pas été trouvé à '{file_path}'. Cette ville sera ignorée.")
            continue

        try:
            with open(file_path) as f:
                data = json.load(f)
        except json.JSONDecodeError as e:
            print(f"Erreur de lecture JSON pour '{file_path}': {e}. Fichier ignoré.")
            continue

        if 'hourly' not in data:
            print(f"Erreur: La clé 'hourly' est manquante dans les données pour {city}. Fichier ignoré.")
            continue
        
        hourly_data = data['hourly']
        
        required_keys = ['time', 'temperature_2m', 'precipitation', 'weather_code', 'wind_speed_10m', 'cloud_cover', 'relative_humidity_2m']
        if not all(k in hourly_data for k in required_keys):
            print(f"Erreur: Une ou plusieurs clés horaires requises sont manquantes pour {city}. Fichier ignoré.")
            print(f"Clés présentes: {hourly_data.keys()}")
            continue

        df = pd.DataFrame({
            'city_key': city, # Utilisation de city_key pour la jointure
            'datetime': hourly_data['time'],
            'temperature': hourly_data['temperature_2m'],
            'precipitation': hourly_data['precipitation'],
            'weather_code': hourly_data['weather_code'],
            'wind_speed': hourly_data['wind_speed_10m'],
            'cloud_cover': hourly_data['cloud_cover'],
            'humidity': hourly_data['relative_humidity_2m']
        })

        df['datetime'] = pd.to_datetime(df['datetime'])
        
        # Filtrer les données pour la période demandée
        df = df[(df['datetime'] >= start_datetime) & (df['datetime'] <= end_datetime)]
        if df.empty:
            print(f"ATTENTION: Aucune donnée pour {city} dans la période {start_date_str} au {end_date_str}.")
            continue

        # --- Nouvelles colonnes pour l'analyse de vacances ---
        df['is_rainy'] = df['precipitation'] > 0.5 
        df['is_sunny'] = df['cloud_cover'] < 20 

        def calculate_feels_like(row):
            T = row['temperature']
            RH = row['humidity']
            wind = row['wind_speed']
            
            if T >= 20 and RH >= 40:
                return -8.784695 + 1.611394*T + 2.338549*RH - 0.146116*T*RH - 0.012308*T**2 - 0.016425*RH**2 + 0.002212*T**2*RH + 0.000725*T*RH**2 - 0.000003582*T**2*RH**2
            elif T < 10 and wind > 5:
                 return T - (wind * 0.5)
            else:
                return T

        df['feels_like_temperature'] = df.apply(calculate_feels_like, axis=1)

        # Création des clés de dimension
        df['date_key'] = df['datetime'].dt.strftime('%Y%m%d').astype(int) # Clé numérique pour la date
        df['datetime_key'] = df['datetime'].dt.strftime('%Y%m%d%H%M%S').astype(np.int64) # Clé numérique pour datetime

        # Sélection et renommage des colonnes pour la table de faits
        fact_df = df[['datetime_key', 'date_key', 'city_key', 'temperature', 'precipitation', 
                      'weather_code', 'wind_speed', 'cloud_cover', 'humidity', 
                      'feels_like_temperature', 'is_rainy', 'is_sunny']]
        
        all_cities_hourly_dfs.append(fact_df)
        print(f"Données transformées et filtrées pour {city} ajoutées à la liste des faits.")

    if not all_cities_hourly_dfs:
        print("Aucune donnée de ville n'a pu être transformée pour la période spécifiée. Les fichiers CSV ne seront pas créés.")
        return

    # Concaténer tous les DataFrames en un seul pour la table de faits
    final_fact_df = pd.concat(all_cities_hourly_dfs, ignore_index=True)
    print(f"Toutes les données de faits horaires combinées.")

    # --- Génération de la Dimension Date (dim_date) et Dimension Time (dim_time) ---
    # Récupérer toutes les dates et heures uniques de la table de faits pour créer les dimensions
    unique_datetimes = final_fact_df['datetime_key'].unique()
    unique_dates = final_fact_df['date_key'].unique()

    # dim_time
    dim_time_df = pd.DataFrame({'datetime_key': unique_datetimes})
    dim_time_df['full_datetime'] = pd.to_datetime(dim_time_df['datetime_key'].astype(str), format='%Y%m%d%H%M%S')
    dim_time_df['hour_of_day'] = dim_time_df['full_datetime'].dt.hour
    dim_time_df['minute_of_hour'] = dim_time_df['full_datetime'].dt.minute # Sera 00 ici
    dim_time_df = dim_time_df.sort_values('datetime_key').reset_index(drop=True)
    dim_time_df.to_csv(os.path.join(data_dir, "dim_time.csv"), index=False)
    print("Dimension temps (dim_time.csv) créée.")

    # dim_date
    dim_date_df = pd.DataFrame({'date_key': unique_dates})
    dim_date_df['full_date'] = pd.to_datetime(dim_date_df['date_key'].astype(str), format='%Y%m%d').dt.date # Garder juste la date
    dim_date_df['day_of_week'] = pd.to_datetime(dim_date_df['date_key'].astype(str), format='%Y%m%d').dt.day_name()
    dim_date_df['day_of_month'] = pd.to_datetime(dim_date_df['date_key'].astype(str), format='%Y%m%d').dt.day
    dim_date_df['month'] = pd.to_datetime(dim_date_df['date_key'].astype(str), format='%Y%m%d').dt.month_name()
    dim_date_df['month_number'] = pd.to_datetime(dim_date_df['date_key'].astype(str), format='%Y%m%d').dt.month
    dim_date_df['year'] = pd.to_datetime(dim_date_df['date_key'].astype(str), format='%Y%m%d').dt.year
    dim_date_df['is_weekend'] = dim_date_df['full_date'].apply(lambda x: x.weekday() >= 5) # Saturday=5, Sunday=6
    dim_date_df = dim_date_df.sort_values('date_key').reset_index(drop=True)
    dim_date_df.to_csv(os.path.join(data_dir, "dim_date.csv"), index=False)
    print("Dimension date (dim_date.csv) créée.")

    # Enregistrer la table de faits
    output_fact_file_path = os.path.join(data_dir, "fact_weather_hourly.csv")
    final_fact_df.to_csv(output_fact_file_path, index=False)
    print(f"Table de faits (fact_weather_hourly.csv) enregistrée dans '{output_fact_file_path}'.")

    # (Optionnel) Recréer le résumé quotidien à partir de la table de faits
    # daily_summary_df = final_fact_df.groupby(['city_key', 'date_key']).agg(
    #     avg_temp=('temperature', 'mean'),
    #     max_temp=('temperature', 'max'),
    #     min_temp=('temperature', 'min'),
    #     total_precipitation=('precipitation', 'sum'),
    #     avg_wind_speed=('wind_speed', 'mean'),
    #     avg_cloud_cover=('cloud_cover', 'mean'),
    #     avg_humidity=('humidity', 'mean'),
    #     rainy_hours=('is_rainy', 'sum'),
    #     sunny_hours=('is_sunny', 'sum')
    # ).reset_index()
    # daily_summary_df.to_csv(os.path.join(data_dir, "fact_weather_daily_summary.csv"), index=False)
    # print(f"Résumé quotidien (fact_weather_daily_summary.csv) enregistré.")