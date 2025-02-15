import pandas as pd
import os
from pathlib import Path

# Ruta del directorio donde están los archivos CSV originales
input_directory = 'Aave_data.csv'
output_directory = '../coin_years'

# Asegúrate de que el directorio de salida existe
Path(output_directory).mkdir(parents=True, exist_ok=True)

# Lista todos los archivos CSV en el directorio
for file_name in os.listdir(input_directory):
    if file_name.endswith('.csv'):
        # Carga el archivo CSV
        file_path = os.path.join(input_directory, file_name)
        data = pd.read_csv(file_path)

        # Procesa cada fila del DataFrame
        for index, row in data.iterrows():
            datetime = pd.to_datetime(row['datetime'])
            year = datetime.year
            month = datetime.month
            symbol = row['symbol']

            # Directorio de salida para la moneda específica y año
            coin_dir = os.path.join(output_directory, symbol, str(year))
            Path(coin_dir).mkdir(parents=True, exist_ok=True)
            
            # Nombre del archivo de salida para el mes específico
            output_file = os.path.join(coin_dir, f'{month:02d}.csv')
            
            # Si el archivo ya existe, carga el contenido existente, de lo contrario crea un DataFrame vacío
            if os.path.exists(output_file):
                month_data = pd.read_csv(output_file)
            else:
                month_data = pd.DataFrame(columns=data.columns)
            
            # Añade la fila al DataFrame del mes
            month_data = month_data.append(row, ignore_index=True)
            
            # Guarda el DataFrame actualizado en el archivo CSV
            month_data.to_csv(output_file, index=False)

print("Proceso completado. Los datos están organizados por moneda y año en carpetas correspondientes.")
