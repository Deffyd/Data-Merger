# data_loading.py

import os
import pandas as pd
import mysql.connector
from pymongo import MongoClient
import warnings

RAW_DATA_DIR = 'data/raw'

def load_data_from_csv(file_path):
    data = pd.read_csv(file_path)
    dt = len(data) - len(data.drop_duplicates())  # Cantidad de duplicados eliminados
    print('_' * 150)
    print(f"\nTotal de datos cargados desde CSV '{file_path}': {len(data)} filas")
    print(f"Documentos duplicados eliminados: {dt}")
    return data

def save_data_to_csv(data, file_name, output_dir):
    try:
        os.makedirs(output_dir, exist_ok=True)
        file_path = os.path.join(output_dir, file_name)
        data.to_csv(file_path, index=False, encoding='utf-8-sig')  # UTF-8 con BOM para asegurar compatibilidad
        print(f"Datos guardados en '{file_path}'.")
        return True
    except Exception as e:
        print(f"Error al guardar datos en CSV: {e}")
        return False

def connect_to_mysql(connection_params):
    try:
        conn = mysql.connector.connect(**connection_params)
        print('_' * 150)
        print("\nConexión establecida a MySQL.")
        return conn
    except mysql.connector.Error as e:
        print(f"Error de conexión a MySQL: {e}")
        return None

def load_data_from_mysql(connection_params, table_name):
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        conn = connect_to_mysql(connection_params)

        if conn is None:
            return None

        try:
            query = f"SELECT * FROM {table_name}"
            data = pd.read_sql(query, conn)
            if 'id' not in data.columns:
                data.rename(columns={'ID': 'id'}, inplace=True)
            duplicates_count = len(data) - len(data.drop_duplicates())
            data.drop_duplicates(inplace=True)
            data['id'] = range(1, len(data) + 1)
            print(f"Total de datos cargados desde MySQL: {len(data)} filas")
            print(f"Datos duplicados eliminados: {duplicates_count}")
            save_data_to_csv(data, f"{table_name}_raw.csv", RAW_DATA_DIR)
            return data
        except mysql.connector.Error as e:
            print(f"Error al cargar datos desde MySQL: {e}")
            return None
        finally:
            conn.close()
            print("Conexión cerrada.")

def connect_to_mongodb(connection_string):
    try:
        client = MongoClient(connection_string)
        print('_' * 150)
        print("\nConexión establecida a MongoDB.")
        return client
    except Exception as e:
        print(f"Error de conexión a MongoDB: {e}")
        return None

def load_data_from_mongodb(client, database_name, collection_name):
    if client is None:
        return None

    try:
        db = client[database_name]
        collection = db[collection_name]
        cursor = collection.find({})
        data = pd.DataFrame(list(cursor))
        if '_id' in data.columns:
            data.rename(columns={'_id': 'id'}, inplace=True)
        duplicates_count = len(data) - len(data.drop_duplicates())
        data.drop_duplicates(inplace=True)
        data['id'] = range(1, len(data) + 1)
        print(f"Total de documentos cargados desde MongoDB: {len(data)} documentos")
        print(f"Documentos duplicados eliminados: {duplicates_count}")
        save_data_to_csv(data, f"{collection_name}_raw.csv", RAW_DATA_DIR)
        return data
    except Exception as e:
        print(f"Error al cargar datos desde MongoDB: {e}")
        return None
    finally:
        client.close()
        print("Conexión cerrada a MongoDB.")