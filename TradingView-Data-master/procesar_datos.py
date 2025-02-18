import pandas as pd

archivos = ["Aave", "Bitcoin", "Cardano", "Dogecoin", "Ethereum", "Polkadot", "Ripple", "Shiba_Inu", "Solana", "Stellar"]

for archivo in archivos:
    df = pd.read_csv(f"{archivo}_data.csv")

    df["datetime"] = pd.to_datetime(df["datetime"])

    df["year"] = df["datetime"].dt.year
    df["month"] = df["datetime"].dt.month
    df["day"] = df["datetime"].dt.day

    df = df.drop(columns=["datetime"])

    for year, data in df.groupby("year"):
        filename = f"{archivo}_{year}.csv"  
        data.to_csv(filename, index=False)  
        print(f"Archivo generado: {filename}")  
