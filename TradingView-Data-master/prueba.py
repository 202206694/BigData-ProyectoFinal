import pandas as pd
import boto3
import os

# Configuración de S3
s3_client = boto3.client('s3', 
                         aws_access_key_id='imat08', 
                         aws_secret_access_key='Realmadrid_15',
                         region_name='eu-south-2') 

archivos = ["Aave", "Bitcoin", "Cardano", "Dogecoin", "Ethereum", "Polkadot", "Ripple", "Shiba_Inu", "Solana", "Stellar"]

# Diccionario de abreviaciones
crypto_dict = {
    "Bitcoin": "BTC",
    "Ethereum": "ETH",
    "Ripple": "XRP",
    "Solana": "SOL",
    "Dogecoin": "DOGE",
    "Cardano": "ADA",
    "Shiba_Inu": "SHB",
    "Polkadot": "DOT",
    "Aave": "AAVE",
    "Stellar":"XLM"
}

# Variable para controlar los lotes de archivos
counter = 0
files_to_upload = []

for archivo in archivos:
    df = pd.read_csv(f"{archivo}_data.csv")

    # Manipulación de datos
    df["datetime"] = pd.to_datetime(df["datetime"])
    df["year"] = df["datetime"].dt.year
    df["month"] = df["datetime"].dt.month
    df["day"] = df["datetime"].dt.day
    df = df.drop(columns=["datetime"])

    # Generar archivos por año
    for year, data in df.groupby("year"):
        # Ignorar los archivos de 2020
        if year == 2020:
            continue

        filename = f"{archivo}_{year}.csv"
        data.to_csv(filename, index=False)
        print(f"Archivo generado: {filename}")
        
        # Almacenar el archivo en la lista para cargar a S3
        files_to_upload.append(filename)
        
        # Subir a S3 en lotes de 5 archivos
        counter += 1
        if counter == 5:
            # Nombre del bucket usando la abreviación en minúsculas
            bucket_name = f"grupo13-{crypto_dict[archivo].lower()}"
            for file in files_to_upload:
                s3_client.upload_file(file, bucket_name, file)
                print(f"Subido a S3: {file} en el bucket {bucket_name}")
            
            # Reiniciar el contador y la lista de archivos
            counter = 0
            files_to_upload = []

# Subir los archivos restantes si hay menos de 5 en el último lote
if files_to_upload:
    for file in files_to_upload:
        bucket_name = f"grupo13-{crypto_dict[archivo].lower()}"  # Último archivo procesado
        s3_client.upload_file(file, bucket_name, file)
        print(f"Subido a S3: {file} en el bucket {bucket_name}")
