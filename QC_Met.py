# 000import libs
import sys
sys.path.insert(0, 'scripts')
import pandas as pd
import psycopg2
from config import *
from data_analysis import *
from data_processing import *
from db_createtables import *
from db_operations import *
#from db_tabledefinitions import *
from external_api import *
from file_operations import *
from qc_params import *
#from qc_controls import *
from qc_process import *
#from sqlalchemy import create_engine
from utils import *
import time
import logging
from tqdm import tqdm  # Importa tqdm

# Lista de pasos en tu proceso
steps = [
    "Copying data",
    "Processing data",
    "Filtering UUIDs",
    "Filtering ten-minute data",
    "Retrieving AEMET data",
    "Retrieving Agrocabildo data",
    "Processing errors and changes"
]

# Create a tqdm instance with the list of steps
progress_bar = tqdm(steps, desc="Overall progress")

logging.basicConfig(filename='process.log', level=logging.INFO, format='%(asctime)s - %(message)s')
# Llamada a la función con rutas de origen y destino

start_time = time.time()

run_robocopy('\\\\192.168.20.87\\StoredValues', 'c:\\temp\\datos')
progress_bar.update(1)

# Print de config básico
print(main_folder_path)
print(stations)
print(filter_by_interval)

if filter_by_interval:
    print("Start Date:", start_date)
    print("End Date:", end_date)
else:
    print("Days Back:", days_back)


get_database_connection ()# Conexión a la base de datos
conn = psycopg2.connect(database_url)
cur = conn.cursor()

# Lista de tablas a truncar
#tables_to_truncate = ['datos_meteo', 'datos_meteo_fix', 'errores', 'registro_procesamiento', 'datos_aemet', 'datos_agrocabildo', 'controles_ejecutados']

# Llamada a la función para truncar tablas
#truncate_tables(conn, tables_to_truncate)

# Llamada a la función para crear tablas e índices
#create_aemet_agrocabildo_tables_and_indices(conn)
#create_tables_and_indices(conn)

df = process_data(conn, main_folder_path, stations, filter_by_interval, days_back, start_date, end_date)
progress_bar.update(1)

df = uuid_filter(df, database_url, qcc=550)
progress_bar.update(1)

df_original = df.copy()
df = filter_ten_minute_data(df, 'fecha')
progress_bar.update(1)
#analyze_data_by_station(df)
#visual_summary_by_station(df)

# Llamada a la función 

df_aemet = get_aemet_api_data(stations_aemet, api_key, url_aemet)
progress_bar.update(1)

df_agrocabildo = get_agrocabildo_api_data(url_agrocabildo, stations_agrocabildo, headers)
progress_bar.update(1)

database_url = "postgresql://postgres:gherrera@localhost:5432/qcmeteo"
#get_database_connection()
# Establecer la conexión a la base de datos
engine = create_engine(database_url)

# Procesar y guardar el DataFrame resultante
# Procesar errores y cambios en el DataFrame
errors_total, df, controls_performed = process_errors_and_changes(engine, df_original, df, variables_and_ranges, df_aemet, df_agrocabildo, conn)
progress_bar.update(1)

# Close the progress bar
progress_bar.close()


# Variables para log
fecha_minima = df['fecha'].min()
fecha_maxima = df['fecha'].max()
end_time = time.time()
total_time_seconds = end_time - start_time
# Convierte el tiempo total a minutos y segundos
total_time_minutes = total_time_seconds // 60
total_time_seconds = total_time_seconds % 60

# Registra los resultados en el log
logging.info('-' * 40)  # Separador
logging.info(f'Rango de fechas: {fecha_minima} a {fecha_maxima}')
logging.info(f'Total de valores en el DataFrame: {len(df)*29}') #29 número de variables
logging.info(f'Total de errores procesados: {len(errors_total)}')
logging.info(f'Tiempo de ejecución: {int(total_time_minutes)} minutos {int(total_time_seconds)} segundos')

input("Presiona Enter para salir...")