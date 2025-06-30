import os
import pandas as pd

def load_data():
    data_dir = "data"
    combined_file_path = os.path.join(data_dir, "all_cities_transformed.csv")

    if not os.path.exists(combined_file_path):
        print(f"Erreur: Le fichier combiné '{combined_file_path}' n'a pas été trouvé. La tâche de transformation a peut-être échoué.")
        return

    try:
        df = pd.read_csv(combined_file_path)
        print(f"Données combinées chargées avec succès depuis '{combined_file_path}'.")
        print(f"Aperçu des 5 premières lignes des données chargées :\n{df.head()}")
        print(f"Informations sur le DataFrame :\n{df.info()}")
        
        # Ici, vous pouvez ajouter des étapes de chargement vers une base de données, un data warehouse, etc.
        # Pour l'instant, nous nous contentons d'afficher les informations pour prouver que le chargement est réussi.

    except Exception as e:
        print(f"Erreur lors du chargement du fichier '{combined_file_path}': {e}")