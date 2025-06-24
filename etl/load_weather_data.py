import pandas as pd

def load_data():
    cities = ['paris', 'new_york', 'tokyo', 'cairo','mahajanga-madagascar']

    for city in cities:
        df = pd.read_csv(f"data/{city}_transformed.csv")
        # Ici je charger vers unema base de données ou laisser les fichiers CSV(izay tiko)
        print(f"{city} - Données prêtes : {len(df)} lignes")
