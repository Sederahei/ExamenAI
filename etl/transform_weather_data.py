import os
import json
import pandas as pd
import numpy as np
from datetime import datetime, timedelta # Importez timedelta

def transform_data(start_date_str: str, end_date_str: str): # Ajout des paramètres de date
    data_dir = "data"
    cities = ['paris', 'new_york', 'tokyo', 'cairo', 'mahajanga-madagascar']
    
    start_datetime = datetime.strptime(start_date_str, '%Y-%m-%d')
    end_datetime = datetime.strptime(end_date_str, '%Y-%m-%d') + timedelta(days=1, microseconds=-1) # Pour inclure toute la journée de fin
    
    all_cities_dfs = []

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
            'city': city,
            'datetime': hourly_data['time'],
            'temperature': hourly_data['temperature_2m'],
            'precipitation': hourly_data['precipitation'],
            'weather_code': hourly_data['weather_code'],
            'wind_speed': hourly_data['wind_speed_10m'],
            'cloud_cover': hourly_data['cloud_cover'],
            'humidity': hourly_data['relative_humidity_2m']
        })

        df['datetime'] = pd.to_datetime(df['datetime'])
        
        # --- Filtrer les données pour la période demandée ---
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
            wind = row['wind_speed'] # Utilisez le vent pour wind chill
            
            # Simple approximation pour la température ressentie
            if T >= 20 and RH >= 40: # Heat Index pour climat chaud et humide
                # Formule simplifiée de Heat Index (approximative)
                return -8.784695 + 1.611394*T + 2.338549*RH - 0.146116*T*RH - 0.012308*T**2 - 0.016425*RH**2 + 0.002212*T**2*RH + 0.000725*T*RH**2 - 0.000003582*T**2*RH**2
            elif T < 10 and wind > 5: # Wind Chill pour climat froid et venteux
                # Formule simplifiée (très approximative, la vraie dépend de T et Wind Speed)
                return T - (wind * 0.5) # Diminue la température ressentie avec le vent
            else:
                return T

        df['feels_like_temperature'] = df.apply(calculate_feels_like, axis=1)

        # Extraction d'informations temporelles
        df['day_of_week'] = df['datetime'].dt.day_name()
        df['hour_of_day'] = df['datetime'].dt.hour
        df['month'] = df['datetime'].dt.month_name()
        df['day'] = df['datetime'].dt.day
        df['date'] = df['datetime'].dt.date # Ajout d'une colonne de date seule pour l'agrégation quotidienne

        all_cities_dfs.append(df)
        print(f"Données transformées et filtrées pour {city} ajoutées à la liste.")

    if not all_cities_dfs:
        print("Aucune donnée de ville n'a pu être transformée pour la période spécifiée. Le fichier CSV combiné ne sera pas créé.")
        return

    final_df = pd.concat(all_cities_dfs, ignore_index=True)
    
    # Pour la visualisation, vous pourriez vouloir des agrégations quotidiennes
    # Exemple : Température moyenne par jour et ville
    daily_summary_df = final_df.groupby(['city', 'date']).agg(
        avg_temp=('temperature', 'mean'),
        max_temp=('temperature', 'max'),
        min_temp=('temperature', 'min'),
        total_precipitation=('precipitation', 'sum'),
        avg_wind_speed=('wind_speed', 'mean'),
        avg_cloud_cover=('cloud_cover', 'mean'),
        avg_humidity=('humidity', 'mean'),
        rainy_hours=('is_rainy', 'sum'),
        sunny_hours=('is_sunny', 'sum')
    ).reset_index()
    daily_summary_df['date'] = pd.to_datetime(daily_summary_df['date']) # Convertir la colonne de date en datetime
    
    # Enregistrer le DataFrame combiné dans un seul fichier CSV (détaillé par heure)
    output_hourly_file_path = os.path.join(data_dir, "all_cities_transformed_hourly.csv")
    final_df.to_csv(output_hourly_file_path, index=False)
    print(f"Toutes les données transformées (horaires) ont été enregistrées dans '{output_hourly_file_path}'.")

    # Enregistrer le DataFrame combiné (résumé quotidien)
    output_daily_file_path = os.path.join(data_dir, "all_cities_transformed_daily_summary.csv")
    daily_summary_df.to_csv(output_daily_file_path, index=False)
    print(f"Les résumés quotidiens ont été enregistrés dans '{output_daily_file_path}'.")