from datetime import datetime, timedelta
import os
import pandas as pd
from db_operations import update_processing_record
import psycopg2
from db_tabledefinitions import rain_columns, average_columns
from utils import create_unique_id

def extract_minute_data(df, date_column, average_columns):
    df[date_column] = pd.to_datetime(df[date_column])
    df = df.sort_values(date_column)
    df['_diferencia'] = df[date_column].diff().dt.total_seconds() / 60
    df_minute_data = df[df['_diferencia'] == 1].copy()
    
    # Establecer los valores de lluvia a 0
    for column in average_columns:
        df_minute_data[column] = 0
    
    return df_minute_data

def build_ten_minute_data(df, date_column,  average_columns):
    df[date_column] = pd.to_datetime(df[date_column])
    df = df.sort_values(date_column)
    df['fecha_diezmin_siguiente'] = (df[date_column] + pd.Timedelta(minutes=1)).dt.floor('10min')
    df_diezminutales = df[df[date_column].dt.minute % 10 == 1].copy()
    print (df_diezminutales)
    # Asegúrate de que todas las columnas_medias estén presentes en el DataFrame, si no, agrégalas con un valor predeterminado
    for columna in  average_columns:
        if columna not in df_diezminutales.columns:
            df_diezminutales[columna] = None  # Puedes cambiar el 0 por el valor predeterminado que desees

    df_diezminutales = df_diezminutales[[date_column] +  average_columns]
    df_diezminutales = df_diezminutales.rename(columns={'fecha_diezmin_siguiente': 'fecha'})
    #df_diezminutales = df_diezminutales.drop(columns=['fecha_diezmin_siguiente'])
    return df_diezminutales

def combine_ten_minute_data(df_original, df_nuevos, date_column):
    df_original = df_original[df_original['_diferencia'] != 1].copy()
    df_combined = pd.concat([df_original, df_nuevos], ignore_index=True)
    df_combined = df_combined.sort_values(date_column).reset_index(drop=True)
    return df_combined

def delete_minute_data(df, date_column):
    df[date_column] = pd.to_datetime(df[date_column])
    df = df.sort_values(date_column)
    df['_diferencia'] = df[date_column].diff().dt.total_seconds() / 60
    df = df[df['_diferencia'] != 1].copy()
    return df

def filter_ten_minute_data(df, date_column):
    # Convertir la columna de fecha a tipo datetime si aún no lo es
    df[date_column] = pd.to_datetime(df[date_column])
    
    # Filtrar las filas donde los minutos son múltiplos de 10
    df_filtered = df[df[date_column].dt.minute % 10 == 0]

    # Asegurarse de que todas las columnas de lluvia y medias estén presentes en el DataFrame
    for column in rain_columns + average_columns:
        if column not in df_filtered.columns:
            df_filtered[column] = None

    # Establecer los valores de lluvia a 0 para los registros diezminutales
    for column in rain_columns:
        df_filtered.loc[df_filtered[date_column].dt.minute % 10 == 0, column] = 0
    
    # Seleccionar solo las columnas deseadas
    df = df_filtered[[date_column] + average_columns]
    
    
    for column in df.columns:
        if df[column].dtype == 'object':
            df[column] = pd.to_numeric(df[column], errors='coerce')
    
    return df

def process_csv(conn, file_path, station, nombre_archivo, mapping_dictionary):
    df = pd.read_csv(file_path, sep=';', decimal=',', encoding='ISO-8859-1')
    df = df[[col for col in df.columns if not col.lower().startswith('unnamed')]]
    df['codigo'] = station
    if 'Fecha' in df.columns:
        df['Fecha'] = pd.to_datetime(df['Fecha'], format='%d/%m/%Y %H:%M:%S', errors='coerce')
        first_date = df['Fecha'].min()
        last_date = df['Fecha'].max()
        date_proc = datetime.now()
    else:
        first_date = last_date = date_proc = None

    # Renombrar las columnas del DataFrame según el mapeo
    df = df.rename(columns=mapping_dictionary)

    # Filtrar solo las columnas presentes en el mapeo
    table_fields = list(mapping_dictionary.values())
    df = df[[col for col in df.columns if col in table_fields]]

    processed_lines = len(df)
    return df, first_date, last_date, processed_lines, date_proc

def process_data(conn, main_folder_path, carpetas_a_procesar, filter_by_interval, dias_atras, start_date=None, end_date=None):
    with conn.cursor() as cur:
        cur.execute("SELECT nombre_original, nombre_transformado FROM tabla_campos_transformados ORDER BY id")
        field_mapping = cur.fetchall()
        mapping_dictionary = {origen: transformado for origen, transformado in field_mapping}

    dataframes = []

    if filter_by_interval:
        start_date = start_date
        end_date = end_date
    else:
        start_date = datetime.now() - timedelta(days=dias_atras)
        end_date = datetime.now()

    for carpeta in carpetas_a_procesar:
        folder_path = os.path.join(main_folder_path, carpeta)
        print(f"Procesando carpeta: {folder_path}")
        for archivo in os.listdir(folder_path):
            if archivo.endswith('.csv'):
                partes_nombre_archivo = archivo.split('_')
                parte_fecha = partes_nombre_archivo[-1].split('.')[0]
                fecha_archivo = datetime.strptime(parte_fecha, '%Y%m%d')
                if filter_by_interval:
                    es_valido = start_date <= fecha_archivo <= end_date
                else:
                    es_valido = fecha_archivo >= start_date
                if es_valido and not archive_processed(conn, carpeta, archivo.split('.')[0]):
                    print(f"Procesando archivo: {archivo}")
                    nombre_archivo = archivo.split('.')[0]
                    file_path = os.path.join(folder_path, archivo)
                    df, primera_fecha, ultima_fecha, lineas_procesadas, fecha_proc = process_csv(conn, file_path, carpeta, nombre_archivo, mapping_dictionary)
                    dataframes.append(df)
                    update_processing_record(conn, carpeta, nombre_archivo, lineas_procesadas, primera_fecha, ultima_fecha, fecha_proc)
                else:
                    print(f"Archivo omitido: {archivo}")

    if dataframes:
        df = pd.concat(dataframes, ignore_index=False)
        df['uuid'] = df.apply(lambda row: create_unique_id(row['fecha'], row['codigo']), axis=1)
        df['fecha_proc'] = datetime.now()
        df['codigo'] = df['codigo'].astype(int)
    else:
        print("No hay archivos CSV para procesar.")
        df = pd.DataFrame()
    
    conn.commit()
      
    return df

def uuid_filter(df, database_url, qcc=550):
    conn = psycopg2.connect(database_url)
    # Consultar la tabla de control para obtener los uuid con qcc igual a 550
    query = f"SELECT uuid FROM controles_ejecutados WHERE qcc = {qcc}"
    uuids_excluir = pd.read_sql(query, conn)

    # Cerrar la conexión a la base de datos
    conn.close()

    # Filtrar el DataFrame
    lista_uuids_excluir = uuids_excluir['uuid'].tolist()
    df_filtrado = df[~df['uuid'].isin(lista_uuids_excluir)]

    # Devolver el DataFrame filtrado
    return df_filtrado

def archive_processed(conn, codigo, nombre_archivo, total_lineas=144):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT lineas_procesadas
            FROM registro_procesamiento
            WHERE codigo = %s AND nombre_archivo = %s
        """, (codigo, nombre_archivo))
        resultado = cur.fetchone()
        if resultado:
            lineas_procesadas = resultado[0]
            return lineas_procesadas >= total_lineas
        else:
            return False
        


