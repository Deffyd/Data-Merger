
## Instalación
1. Clona el repositorio:
    ```bash
    git clone https://github.com/Deffyd/Data-Merger.git
    cd Data-Merger
    ```

2. Crea un entorno virtual y activa el entorno:
    ```bash
    python -m venv env
    source env/bin/activate  # En Windows usa `env\Scripts\activate`
    ```

3. Instala las dependencias:
    ```bash
    pip install -r requirements.txt
    ```

## Uso
### Carga de Datos
El script `data_loading.py` proporciona funciones para cargar datos desde archivos CSV, bases de datos MySQL y MongoDB.

- **Carga desde CSV:**
    ```python
    from scripts.data_loading import load_data_from_csv

    csv_data = load_data_from_csv('data/raw/data.csv')
    ```

- **Carga desde MySQL:**
    ```python
    from scripts.data_loading import load_data_from_mysql

    mysql_connection_params = {
        'host': 'localhost',
        'user': 'root',
        'password': '',
        'database': 'data_loading',
        'port': 3306,
    }
    mysql_data = load_data_from_mysql(mysql_connection_params, 'mock_data')
    ```

- **Carga desde MongoDB:**
    ```python
    from scripts.data_loading import connect_to_mongodb, load_data_from_mongodb

    mongodb_connection_string = 'mongodb://localhost:27017'
    mongodb_client = connect_to_mongodb(mongodb_connection_string)
    mongodb_data = load_data_from_mongodb(mongodb_client, 'mongo', 'data')
    ```

### Procesamiento de Datos
El script `data_processing.py` combina múltiples DataFrames y los guarda en un archivo CSV.

- **Combinar y guardar datos:**
    ```python
    from scripts.data_processing import combine_data, save_combined_data_to_csv

    combined_data, duplicates_count = combine_data(csv_data, mysql_data, mongodb_data)
    save_combined_data_to_csv(combined_data, 'combined_data.csv', 'data/processed')
    ```

### Ejecutar el Script Principal
El script `main.py` proporciona un ejemplo de cómo cargar, combinar y guardar datos:
```bash
python main.py
