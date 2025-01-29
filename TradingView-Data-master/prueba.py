import pandas as pd

# Cargar el archivo CSV original
df = pd.read_csv("Bitcoin_data.csv")

# Convertir la columna datetime a tipo datetime
df["datetime"] = pd.to_datetime(df["datetime"])

# Agrupar los datos por año y crear archivos separados
for year, data in df.groupby(df["datetime"].dt.year):
    filename = f"Bitcoin_{year}.csv"  # Nombre del archivo para cada año
    data.to_csv(filename, index=False)  # Guardar en CSV sin índice
    print(f"Archivo generado: {filename}")  # Confirmación en consola
