from datetime import datetime
import pandas as pd
from qc_controls import detect_locks,out_of_range,check_coherence,check_variable_changes,check_rain_with_mapping
from qc_params import columns_to_check,station_mapping
from db_operations import upsert_dataframe,insert_error
from utils import create_unique_id

def process_errors_and_changes(engine, df_original, df, variables_y_rangos, df_aemet, df_agrocabildo, conn):
      
    total_errors = []
    controls_performed = []

    dataframes = [df, df_original]

    # Vuelve a crear uuids y reset de index
    for dataframe in dataframes:
        dataframe['uuid'] = dataframe.apply(lambda row: create_unique_id(row['fecha'], row['codigo']) if pd.isnull(row['uuid']) else row['uuid'], axis=1)
        dataframe.reset_index(drop=True, inplace=True)

    #df['uuid'] = df.apply(lambda row: create_unique_id(row['fecha'], row['codigo']) if pd.isnull(row['uuid']) else row['uuid'], axis=1)
    
    #df.reset_index(drop=True, inplace=True)

        # Ajustar valores numéricos en df_original y df para evitar desbordamiento de campo numeric
    limite_absoluto = 9999.999  # Ajusta este valor según la precisión y escala de tu campo numérico
    for df_actual in [df_original, df]:
        for column in df_actual.columns:
            if df_actual[column].dtype == 'float64':  # Verifica si la columna es numérica y no entera
                df_actual[column] = df_actual[column].round(3)  # Redondea a 3 decimales
                df_actual[column] = df_actual[column].clip(-limite_absoluto, limite_absoluto)

    # Procesar errores de rango para todo el DataFrame
    blocking_errors, df = detect_locks(df, columns_to_check)
    total_errors.extend(blocking_errors)
    controls_performed.extend([(uuid, 510) for uuid in df['uuid'].tolist()])
    
    range_errors, df = out_of_range(df)
    total_errors.extend(range_errors)
    controls_performed.extend([(uuid, 520) for uuid in df['uuid'].tolist()])

    # Procesar errores de coherencia para todo el DataFrame modificado
    coherence_errors, df = check_coherence(df)
    total_errors.extend(coherence_errors)
    controls_performed.extend([(uuid, 530) for uuid in df['uuid'].tolist()])

    # Procesar errores de cambios de variables para todo el DataFrame modificado
    variable_changes_errors, df = check_variable_changes(df, variables_y_rangos)
    total_errors.extend(variable_changes_errors)
    controls_performed.extend([(uuid, 540) for uuid in df['uuid'].tolist()])

    rain_errors = check_rain_with_mapping(df, df_aemet, df_agrocabildo, station_mapping)
    total_errors.extend(rain_errors)
    controls_performed.extend([(uuid, 550) for uuid in df['uuid'].tolist()])

    # Insertar datos iniciales en la tabla 'datos_meteo'
    upsert_dataframe(df_original, 'datos_meteo', engine, unique_columns=['codigo', 'fecha'])
    # Actualizar datos en la tabla 'datos_meteo_fix' con el DataFrame modificado
    upsert_dataframe(df, 'datos_meteo_fix', engine, unique_columns=['codigo', 'fecha'])
    upsert_dataframe(df_aemet, 'datos_aemet', engine, unique_columns=['uuid'])
    upsert_dataframe(df_agrocabildo, 'datos_agrocabildo', engine, unique_columns=['uuid'])

    # Filtrar los errores duplicados por UUID, fecha, QCC y variable
    unique_errors = []
    for error in total_errors:
        if error not in unique_errors:
            unique_errors.append(error)
    print(f"Número total de errores únicos: {len(unique_errors)}")

    # Insertar los errores únicos en la tabla errores
    
    for error in unique_errors:
        insert_error(conn, error)

    controls_performed = pd.DataFrame(controls_performed, columns=['uuid', 'qcc'])
    controls_performed = controls_performed.groupby('uuid')['qcc'].max().reset_index()
    controls_performed = controls_performed.drop_duplicates(subset=['uuid'])
    controls_performed['fecha_proc'] = datetime.now()
    upsert_dataframe(controls_performed, 'controles_ejecutados', engine, unique_columns=['uuid'])

    # Confirmar cambios
    conn.commit()

    return total_errors, df, controls_performed