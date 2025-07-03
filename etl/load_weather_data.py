import os
import pandas as pd

def load_data(start_date_str: str, end_date_str: str):
    data_dir = "data"
    fact_file_path = os.path.join(data_dir, "fact_weather_hourly.csv")
    dim_date_file_path = os.path.join(data_dir, "dim_date.csv")
    dim_time_file_path = os.path.join(data_dir, "dim_time.csv")
    dim_city_file_path = os.path.join(data_dir, "dim_city.csv")
    dim_weather_code_file_path = os.path.join(data_dir, "dim_weather_code.csv")

    # Charger la table de faits
    if not os.path.exists(fact_file_path):
        print(f"Erreur: Le fichier de faits '{fact_file_path}' n'a pas été trouvé. La tâche de transformation a peut-être échoué.")
        return
    try:
        fact_df = pd.read_csv(fact_file_path)
        print(f"Table de faits chargée : '{fact_file_path}' (Lignes: {len(fact_df)})")
        print(f"Aperçu :\n{fact_df.head()}")
    except Exception as e:
        print(f"Erreur lors du chargement de la table de faits '{fact_file_path}': {e}")
        return

    # Charger les tables de dimensions et les afficher (pour vérification)
    for dim_path in [dim_date_file_path, dim_time_file_path, dim_city_file_path, dim_weather_code_file_path]:
        if os.path.exists(dim_path):
            try:
                dim_df = pd.read_csv(dim_path)
                print(f"\nDimension chargée : '{dim_path}' (Lignes: {len(dim_df)})")
                print(f"Aperçu :\n{dim_df.head()}")
            except Exception as e:
                print(f"Erreur lors du chargement de la dimension '{dim_path}': {e}")
        else:
            print(f"ATTENTION: La dimension '{dim_path}' n'a pas été trouvée.")
    
    print("\nToutes les données (faits et dimensions) sont chargées et prêtes pour l'analyse.")
    # Ici, vous pourriez pousser ces DataFrames vers une base de données si vous en aviez une.
    # Pour l'instant, la preuve de chargement est l'affichage.