import os

import json
import pandas as pd

def transform_data():
    cities = ['paris', 'new_york', 'tokyo', 'cairo', 'mahajanga-madagascar']

    for city in cities:
        with open(f"data/{city}_raw.json") as f:
            data = json.load(f)

        temperatures = data['hourly']['temperature_2m']
        precipitation = data['hourly']['precipitation']
        time = data['hourly']['time']

        df = pd.DataFrame({
            'datetime': time,
            'temperature': temperatures,
            'precipitation': precipitation
        })

        df['datetime'] = pd.to_datetime(df['datetime'])
        df['is_rainy'] = df['precipitation'] > 1.0

        df.to_csv(f"data/{city}_transformed.csv", index=False)

