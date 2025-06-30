import os
import pandas as pd

import pandas as pd

def load_data():
    cities = ['paris', 'new_york', 'tokyo', 'cairo','mahajanga-madagascar']

    for city in cities:
        df = pd.read_csv(f"data/{city}_transformed.csv")

