import os
import pandas as pd

def load_data(start_date_str: str, end_date_str: str): # Ajout des paramètres pour la cohérence
    data_dir = "data"
    combined_file_path = os.path.join(data_dir, "all_cities_transformed_hourly.csv") # Charger le fichier horaire

    if not os.path.exists(combined_file_path):
        print(f"Erreur: Le fichier combiné '{combined_file_path}' n'a pas été trouvé. La tâche de transformation a peut-être échoué.")
        return

    try:
        df = pd.read_csv(combined_file_path)
        print(f"Données combinées chargées avec succès depuis '{combined_file_path}'.")
        print(f"Aperçu des 5 premières lignes des données chargées :\n{df.head()}")
        print(f"Informations sur le DataFrame :\n{df.info()}")
        
        # Vous pouvez également charger le résumé quotidien si vous en avez besoin ici
        daily_summary_file_path = os.path.join(data_dir, "all_cities_transformed_daily_summary.csv")
        if os.path.exists(daily_summary_file_path):
            daily_df = pd.read_csv(daily_summary_file_path)
            print(f"\nAperçu du résumé quotidien :\n{daily_df.head()}")

    except Exception as e:
        print(f"Erreur lors du chargement du fichier '{combined_file_path}': {e}")