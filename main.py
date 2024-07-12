# main.py

import pandas as pd
from scripts.data_loading import (
    load_data_from_csv,
    load_data_from_mysql,
    load_data_from_mongodb,
    connect_to_mongodb,
    save_data_to_csv
)
from scripts.data_processing import combine_data, save_combined_data_to_csv

# Directorios donde se guardarán los datos
RAW_DATA_DIR = 'data/raw'
PROCESSED_DATA_DIR = 'data/processed'

if __name__ == "__main__":
    # Ejemplo de carga desde CSV
    csv_file_path = 'data/raw/data.csv'
    csv_data = load_data_from_csv(csv_file_path)
    print("\nDatos cargados desde CSV:")
    print(csv_data.head())

    # Ejemplo de carga desde MySQL
    mysql_connection_params = {
        'host': 'localhost',
        'user': 'root',
        'password': '',
        'database': 'data_loading',
        'port': 3306,
    }
    mysql_data = load_data_from_mysql(mysql_connection_params, 'mock_data')
    if mysql_data is not None:
        print("\nDatos cargados desde MySQL:")
        print(mysql_data.head())

    # Ejemplo de carga desde MongoDB
    mongodb_connection_string = 'mongodb://localhost:27017'
    mongodb_client = connect_to_mongodb(mongodb_connection_string)
    if mongodb_client is not None:
        mongodb_data = load_data_from_mongodb(mongodb_client, 'mongo', 'data')
        if mongodb_data is not None:
            print("\nDatos cargados desde MongoDB:")
            print(mongodb_data.head())
            print('_' * 150)

    # Combinar todos los DataFrames en uno solo
    combined_data, duplicates_count = combine_data(csv_data, mysql_data, mongodb_data)

    # Guardar el DataFrame combinado en un archivo CSV en data/processed
    combined_file_name = 'combined_data.csv'
    save_combined_data_to_csv(combined_data, combined_file_name, PROCESSED_DATA_DIR)

    # Mostrar una muestra de los datos procesados combinados
    print("\nMuestra de los datos procesados combinados:")
    print(combined_data.sample(10))  # Muestra aleatoria de 10 filas del DataFrame combinado
    print('_' * 150)

    # Información sobre duplicados
    print(f"\nTotal de duplicados eliminados en combined_data: {duplicates_count}")
    print(f"Total de datos guardados en combined_data: {len(combined_data)}")