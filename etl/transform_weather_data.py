import os
import json
import pandas as pd
import numpy as np # Pour les calculs d'indice de chaleur

def transform_data():
    data_dir = "data"
    cities = ['paris', 'new_york', 'tokyo', 'cairo', 'mahajanga-madagascar']
    
    all_cities_dfs = [] # Liste pour stocker les DataFrames de chaque ville

    for city in cities:
        file_path = os.path.join(data_dir, f"{city}_raw.json")
        
        if not os.path.exists(file_path):
            print(f"ATTENTION: Le fichier brut pour '{city}' n'a pas été trouvé à '{file_path}'. Cette ville sera ignorée.")
            continue # Passe à la ville suivante si le fichier n'existe pas

        try:
            with open(file_path) as f:
                data = json.load(f)
        except json.JSONDecodeError as e:
            print(f"Erreur de lecture JSON pour '{file_path}': {e}. Fichier ignoré.")
            continue

        # Vérifier si les clés existent avant d'accéder
        if 'hourly' not in data:
            print(f"Erreur: La clé 'hourly' est manquante dans les données pour {city}. Fichier ignoré.")
            continue
        
        hourly_data = data['hourly']
        
        required_keys = ['time', 'temperature_2m', 'precipitation', 'weather_code', 'wind_speed_10m', 'cloud_cover', 'relative_humidity_2m']
        if not all(k in hourly_data for k in required_keys):
            print(f"Erreur: Une ou plusieurs clés horaires requises sont manquantes pour {city}. Fichier ignoré.")
            print(f"Clés présentes: {hourly_data.keys()}")
            continue

        time = hourly_data['time']
        temperatures = hourly_data['temperature_2m']
        precipitation = hourly_data['precipitation']
        weather_codes = hourly_data['weather_code']
        wind_speeds = hourly_data['wind_speed_10m']
        cloud_covers = hourly_data['cloud_cover']
        humidities = hourly_data['relative_humidity_2m']

        df = pd.DataFrame({
            'city': city, # Ajout de la colonne ville
            'datetime': time,
            'temperature': temperatures,
            'precipitation': precipitation,
            'weather_code': weather_codes,
            'wind_speed': wind_speeds,
            'cloud_cover': cloud_covers,
            'humidity': humidities
        })

        df['datetime'] = pd.to_datetime(df['datetime'])
        
        # --- Nouvelles colonnes pour l'analyse de vacances ---
        df['is_rainy'] = df['precipitation'] > 0.5 # Pluie significative (ajuster seuil si besoin)
        
        # Un simple indicateur d'ensoleillement (moins de 20% de nuages)
        df['is_sunny'] = df['cloud_cover'] < 20 

        # Calcul d'un indice de chaleur simple (formule de Steadman pour humidex ou humidex simplifié)
        # Note: ceci est une simplification, des formules plus précises existent mais sont plus complexes.
        # Basé sur: Heat Index = -8.78469475556 + 1.61139411*T + 2.33854883889*RH - 0.14611605*T*RH - 0.012308094*T**2 - 0.01642482778*RH**2 + 0.002211732*T**2*RH + 0.00072546*T*RH**2 - 0.000003582*T**2*RH**2
        # Où T est en Celsius, RH en pourcentage.
        # Ou une version plus simple:
        # df['feels_like_temperature'] = df['temperature'] + 0.5 * (df['humidity'] / 100 * df['temperature'] - 10) # Formule très simplifiée
        
        # Utilisons une formule plus commune pour l'indice de chaleur si T > 20C et RH > 40% (Approximation NOAA)
        # Pour des températures plus basses, la température ressentie est souvent la température réelle ou ajustée par le vent.
        def calculate_feels_like(row):
            T = row['temperature']
            RH = row['humidity']
            if T >= 20 and RH >= 40: # Seulement si les conditions sont propices à un "heat index"
                # Formule simplifiée de Heat Index (approximative)
                return -8.784695 + 1.611394*T + 2.338549*RH - 0.146116*T*RH - 0.012308*T**2 - 0.016425*RH**2 + 0.002212*T**2*RH + 0.000725*T*RH**2 - 0.000003582*T**2*RH**2
            elif row['wind_speed'] > 10: # Si vent significatif, ajuster par le facteur éolien (wind chill)
                 # C'est une approximation, pour le vent froid, la formule est plus complexe et dépend des unités
                 # Pour simplifier, si le vent est fort, la température ressentie sera plus froide
                 return T - (row['wind_speed'] / 5) * 1.5 # Facteur arbitraire pour montrer l'effet
            else:
                return T # Par défaut, la température ressentie est la température réelle

        df['feels_like_temperature'] = df.apply(calculate_feels_like, axis=1)

        # Extraction d'informations temporelles
        df['day_of_week'] = df['datetime'].dt.day_name()
        df['hour_of_day'] = df['datetime'].dt.hour
        df['month'] = df['datetime'].dt.month_name()
        df['day'] = df['datetime'].dt.day


        all_cities_dfs.append(df)
        print(f"Données transformées pour {city} ajoutées à la liste.")

    if not all_cities_dfs:
        print("Aucune donnée de ville n'a pu être transformée. Le fichier CSV combiné ne sera pas créé.")
        return # Quitte la fonction si aucune donnée n'a été traitée

    # Concaténer tous les DataFrames en un seul
    final_df = pd.concat(all_cities_dfs, ignore_index=True)
    print(f"Toutes les données de {len(all_cities_dfs)} villes ont été combinées.")

    # Enregistrer le DataFrame combiné dans un seul fichier CSV
    output_file_path = os.path.join(data_dir, "all_cities_transformed.csv")
    final_df.to_csv(output_file_path, index=False)
    print(f"Toutes les données transformées ont été enregistrées dans '{output_file_path}'.")