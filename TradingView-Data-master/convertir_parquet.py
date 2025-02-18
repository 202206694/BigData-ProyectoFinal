import os
import pandas as pd

input_folder = 'csv'
output_folder = 'parquet'

if not os.path.exists(output_folder):
    os.makedirs(output_folder)

cryptos = ["Aave", "Bitcoin", "Cardano", "Dogecoin", "Ethereum", "Polkadot", "Ripple", "Shiba_Inu", "Solana", "Stellar"]
years = [2021, 2022, 2023, 2024, 2025]

for crypto in cryptos:
    for year in years:
        csv_filename = f"{crypto}_{year}.csv"
        
        csv_path = os.path.join(input_folder, csv_filename)
        if os.path.exists(csv_path):
            df = pd.read_csv(csv_path)

            parquet_filename = f"{crypto}_{year}.parquet"
            parquet_path = os.path.join(output_folder, parquet_filename)

            df.to_parquet(parquet_path, engine='pyarrow')

            print(f"Archivo {csv_filename} convertido a {parquet_filename}")
        else:
            print(f"Archivo {csv_filename} no encontrado.")
