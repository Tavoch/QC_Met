from datetime import datetime
import pandas as pd
from qc_params import ranges,folders_ranges


def check_variable_changes(df, variables_and_ranges):

    errors = []

    for variable, config in variables_and_ranges.items():
        qcc = config['qcc']
        back_limit = config['limite_atras']
        forward_limit = config['limite_adelante']
        change_threshold = config['umbral_cambio']

        for i in range(len(df)):
            current_value = df.iloc[i][variable]
            date_value = df.iloc[i]['fecha']
            current_station_code = df.iloc[i]['codigo']

            # Buscar hacia atrás
            j = i - 1
            search_limit_backward = max(0, i - back_limit)
            while j >= search_limit_backward and (pd.isna(df.iloc[j][variable]) or df.iloc[j]['codigo'] != current_station_code):
                j -= 1
            if j >= search_limit_backward:
                previous_value = df.iloc[j][variable]
                if not pd.isna(current_value) and not pd.isna(previous_value) and abs(current_value - previous_value) > change_threshold:
                    errors.append({
                        'uuid': str(df.iloc[i]['uuid']),
                        'fecha': date_value,
                        'codigo': current_station_code,
                        'fecha_proc': datetime.now(),
                        'qcc': int(qcc),
                        'variable_1': variable,
                        'value_1': current_value,
                        'variable_2': variable,
                        'value_2': previous_value,
                        'value_fix': None
                    })
                    df.iloc[i, df.columns.get_loc(variable)] = None

            # Buscar hacia adelante (si es necesario)
            k = i + 1
            search_limit_forward = min(len(df), i + forward_limit + 1)
            while k < search_limit_forward and (pd.isna(df.iloc[k][variable]) or df.iloc[k]['codigo'] != current_station_code):
                k += 1
            if k < search_limit_forward:
                next_value = df.iloc[k][variable]
                if not pd.isna(current_value) and not pd.isna(next_value) and abs(current_value - next_value) > change_threshold:
                    errors.append({
                        'uuid': str(df.iloc[i]['uuid']),
                        'fecha': date_value,
                        'codigo': current_station_code,
                        'fecha_proc': datetime.now(),
                        'qcc': int(qcc),
                        'variable_1': variable,
                        'value_1': current_value,
                        'variable_2': variable,
                        'value_2': next_value,
                        'value_fix': None
                    })
                    df.iloc[i, df.columns.get_loc(variable)] = None

    return errors, df

def out_of_range(df):
    errors = []
    
    for index, row in df.iterrows():
        station = row['codigo']
        station_ranges = folders_ranges.get(str(station), [])
        
        for station_range in station_ranges:
            range_limit = ranges[station_range]
            for variable, limits in range_limit.items():
                for limit_type, values in limits.items():
                    variable_value = row[limit_type]
                    if pd.isna(variable_value):
                        errors.append({
                            'uuid': str(row['uuid']),
                            'fecha': row['fecha'],
                            'codigo': station,
                            'fecha_proc': datetime.now(),
                            'qcc': 520,
                            'variable_1': limit_type,
                            'value_1': None,
                            'variable_2': None,
                            'value_2': None,
                            'value_fix': None
                        })
                    elif not (values['min'] <= variable_value <= values['max']):
                        errors.append({
                            'uuid': str(row['uuid']),
                            'fecha': row['fecha'],
                            'codigo': station,
                            'fecha_proc': datetime.now(),
                            'qcc': 521,
                            'variable_1': limit_type,
                            'value_1': variable_value,
                            'variable_2': None,
                            'value_2': None,
                            'value_fix': None
                        })
                        df.at[index, limit_type] = None
    return errors, df

def check_rain_with_mapping(df, df_aemet, df_agrocabildo, station_mapping):
    errors = []

    # 'fecha' en df_aemet y df_agrocabildo tz-aware
    if df_aemet['fecha'].dt.tz is None:
        df_aemet['fecha'] = df_aemet['fecha'].dt.tz_localize('UTC', ambiguous='infer')
    else:
        df_aemet['fecha'] = df_aemet['fecha'].dt.tz_convert('UTC')

    if df_agrocabildo['fecha'].dt.tz is None:
        df_agrocabildo['fecha'] = df_agrocabildo['fecha'].dt.tz_localize('UTC', ambiguous='infer')
    else:
        df_agrocabildo['fecha'] = df_agrocabildo['fecha'].dt.tz_convert('UTC')

    for index, row in df.iterrows():
        date_time = row['fecha']
        # date_time a UTC si es tz-naive
        if date_time.tzinfo is None:
            date_time = date_time.tz_localize('UTC')

        station = str(row['codigo'])
        rain_fields = station_mapping[station]['campos_lluvia']

        # Comprobar cada campo de lluvia
        for rain_field in rain_fields:
            final_rain = row[rain_field]
            is_rain_api = False
            variable_api = None
            value_api = None

            # Comprobar lluvia en AEMET
            if station_mapping[station]['aemet']:
                station_aemet = station_mapping[station]['aemet']
                aemet_data = df_aemet[(df_aemet['codigo'] == station_aemet) & (df_aemet['fecha'] >= date_time - pd.Timedelta(hours=1)) & (df_aemet['fecha'] <= date_time + pd.Timedelta(hours=1))]
                if aemet_data['lluvia_acu'].sum() > 0:
                    is_rain_api = True
                    variable_api = 'lluvia_acu'
                    value_api = aemet_data['lluvia_acu'].sum()

            # Comprobar lluvia en Agrocabildo
            for station_agrocabildo in station_mapping[station]['agrocabildo']:
                agrocabildo_data = df_agrocabildo[(df_agrocabildo['codigo'] == station_agrocabildo) & (df_agrocabildo['fecha'] >= date_time - pd.Timedelta(minutes=30)) & (df_agrocabildo['fecha'] <= date_time + pd.Timedelta(minutes=30))]
                if agrocabildo_data['lluvia_acu'].sum() > 0:
                    is_rain_api = True
                    variable_api = 'lluvia_acu'
                    value_api = agrocabildo_data['lluvia_acu'].sum()
                    break  # Si hay lluvia en una estación, no es necesario verificar las demás

            # Registrar errores
            if final_rain > 0 and not is_rain_api:
                # Error: Hay lluvia en tus datos pero no en las APIs
                errors.append({
                    'uuid': str(row['uuid']),
                    'fecha': date_time,
                    'codigo': station,
                    'qcc': 550,
                    'variable_1': rain_field,
                    'value_1': final_rain,
                    'variable_2': variable_api,
                    'value_2': (value_api) if value_api is not None else None,
                    'value_fix': 0,
                    'error': f'Hay lluvia en {rain_field} pero no en las APIs'
                })
            elif final_rain == 0 and is_rain_api:
                # Error: No hay lluvia en tus datos pero sí en las APIs
                errors.append({
                    'uuid': str(row['uuid']),
                    'fecha': date_time,
                    'codigo': station,
                    'qcc': 551,
                    'variable_1': rain_field,
                    'value_1': final_rain,
                    'variable_2': variable_api,
                    'value_2': (value_api) if value_api is not None else None,
                    'value_fix': None,
                    'error': f'No hay lluvia en {rain_field} pero sí en las APIs'
                    })

    # Actualizar los valores de lluvia en el DataFrame con lluvia_fix
    for error in errors:
        if 'value_fix' in error and error['value_fix'] is not None:
            row_index = df.index[(df['uuid'] == error['uuid']) & (df['fecha'] == error['fecha'])]
            if len(row_index) > 0:
                df.at[row_index[0], error['variable_1']] = error['value_fix']

    return errors

def check_rain_with_mapping_(df, df_aemet, df_agrocabildo, station_mapping):
    errors = []

    # fecha' en df_aemet sea tz-aware
    if df_aemet['fecha'].dt.tz is None:
        df_aemet['fecha'] = df_aemet['fecha'].dt.tz_localize('UTC', ambiguous='infer')
    else:
        df_aemet['fecha'] = df_aemet['fecha'].dt.tz_convert('UTC')

    # 'fecha' en df_agrocabildo sea tz-aware
    if df_agrocabildo['fecha'].dt.tz is None:
        df_agrocabildo['fecha'] = df_agrocabildo['fecha'].dt.tz_localize('UTC', ambiguous='infer')
    else:
        df_agrocabildo['fecha'] = df_agrocabildo['fecha'].dt.tz_convert('UTC')

    for index, row in df.iterrows():
        date_time = row['fecha']
        # Localiza date_time a UTC si es tz-naive
        if date_time.tzinfo is None:
            date_time = date_time.tz_localize('UTC')

        station = str(row['codigo'])
        rain_fields = station_mapping[station]['campos_lluvia']


    for index, row in df.iterrows():
        date_time = row['fecha']
        station = str(row['codigo'])
        rain_fields = station_mapping[station]['campos_lluvia']
        

        # Comprobar cada campo de lluvia
        for rain_field in rain_fields:
            final_rain = row[rain_field]
            is_rain_api = False
            variable_api = None
            value_api = None

            # Comprobar lluvia en AEMET
            if station_mapping[station]['aemet']:
                station_aemet = station_mapping[station]['aemet']
                aemet_data = df_aemet[(df_aemet['codigo'] == station_aemet) & (df_aemet['fecha'] >= date_time - pd.Timedelta(hours=1)) & (df_aemet['fecha'] <= date_time + pd.Timedelta(hours=1))]
                if aemet_data['lluvia_acu'].sum() > 0:
                    is_rain_api = True
                    variable_api = 'lluvia_acu'
                    value_api = aemet_data['lluvia_acu'].sum()

            # Comprobar lluvia en Agrocabildo
            for station_agrocabildo in station_mapping[station]['agrocabildo']:
                agrocabildo_data = df_agrocabildo[(df_agrocabildo['codigo'] == station_agrocabildo) & (df_agrocabildo['fecha'] >= date_time - pd.Timedelta(minutes=30)) & (df_agrocabildo['fecha'] <= date_time + pd.Timedelta(minutes=30))]
                if agrocabildo_data['lluvia_acu'].sum() > 0:
                    is_rain_api = True
                    variable_api = 'lluvia_acu'
                    value_api = agrocabildo_data['lluvia_acu'].sum()
                    break  # Si hay lluvia en una estación, no es necesario verificar las demás

            # Registrar errores
            if final_rain > 0 and not is_rain_api:
                # Error: Hay lluvia en tus datos pero no en las APIs
                errors.append({
                    'uuid': str(row['uuid']),
                    'fecha': date_time,
                    'codigo': station,
                    'qcc': 550,
                    'variable_1': rain_field,
                    'value_1': final_rain,
                    'variable_2': variable_api,
                    'value_2': (value_api) if value_api is not None else None,
                    'value_fix': 0,
                    'error': f'Hay lluvia en {rain_field} pero no en las APIs'
                })
            elif final_rain == 0 and is_rain_api:
                # Error: No hay lluvia en tus datos pero sí en las APIs
                errors.append({
                    'uuid': str(row['uuid']),
                    'fecha': date_time,
                    'codigo': station,
                    'qcc': 551,
                    'variable_1': rain_field,
                    'value_1': final_rain,
                    'variable_2': variable_api,
                    'value_2': (value_api) if value_api is not None else None, #float(value_api) if value_api is not None else None,
                    'value_fix': None,
                    'error': f'No hay lluvia en {rain_field} pero sí en las APIs'
                    })

    # Actualizar los valores de lluvia en el DataFrame con lluvia_fix
    for error in errors:
        if 'value_fix' in error and error['value_fix'] is not None:
            row_index = df.index[(df['uuid'] == error['uuid']) & (df['fecha'] == error['fecha'])]
            if len(row_index) > 0:
                df.at[row_index[0], error['variable_1']] = error['value_fix']

    return errors
# Funciones de actualización
def update_one_null_other_not(df, index, var1, var2, value, errors, qcc, message):
    value1 = df.loc[index, var1]
    value2 = df.loc[index, var2]
    if not pd.isna(value1) and pd.isna(value2):
        df.loc[index, var1] = None
        errors.append({
            'uuid': str(df.loc[index, 'uuid']),
            'fecha': df.loc[index, 'fecha'],
            'codigo': df.loc[index, 'codigo'],
            'fecha_proc': datetime.now(),
            'qcc': int(qcc),
            'variable_1': var1,
            'value_1': value1,
            'variable_2': var2,
            'value_2': None,
            'value_fix': None
        })
        return True
    return False

def update_greater_less_than_x(df, index, var1, var2, x, errors, qcc, message):
    value1 = df.loc[index, var1]
    value2 = df.loc[index, var2]
    if value1 > 0 and value2 < x:
        df.loc[index, var2] = 0
        errors.append({
            'uuid': str(df.loc[index, 'uuid']),
            'fecha': df.loc[index, 'fecha'],
            'codigo': df.loc[index, 'codigo'],
            'fecha_proc': datetime.now(),
            'qcc': int(qcc),
            'variable_1': var1,
            'value_1': value1,
            'variable_2': var2,
            'value_2': value2,
            'value_fix': 0
        })
        return True
    return False

def update_v1_greater_v2(df, index, var1, var2, value, errors, qcc, message):
    value1 = df.loc[index, var1]
    value2 = df.loc[index, var2]
    if pd.notna(value1) and pd.notna(value2) and value1 - value2 > 0.001:
    #if valor1 is not None and valor2 is not None and valor1 - valor2 > 0.001:
        df.loc[index, var1] = None
        df.loc[index, var2] = None
        errors.append({
            'uuid': str(df.loc[index, 'uuid']),
            'fecha': df.loc[index, 'fecha'],
            'codigo': df.loc[index, 'codigo'],
            'fecha_proc': datetime.now(),
            'qcc': int(qcc),
            'variable_1': var1,
            'value_1': None,
            'variable_2': var2,
            'value_2': None,
            'value_fix': None
        })
        return True
    return False

# Verificación de coherencia
def check_coherence(df):
    errors = []
    params_coherence = [
        ('temp_ai_min', 'temp_ai_med', 'temp_ai_min > temp_ai_med', 530, update_v1_greater_v2),
        ('temp_ai_min', 'temp_ai_max', 'temp_ai_min > temp_ai_max', 530, update_v1_greater_v2),
        ('vviento_med', 'vviento_max', 'vviento_med > vviento_max', 530, update_v1_greater_v2),
        (['lluvia_acu', 'lluvia1_acu', 'lluvia2_acu'],'hum_rel_med', 'hum_rel_med <= 50 y lluvia > 0', 540, update_greater_less_than_x, 50),
        ('procio_med', 'temp_ai_med', 'temp_ai_med es nula', 540, update_one_null_other_not)
    ]
    
    for param in params_coherence:
        var1, var2, message, qcc, update_function = param[:5]
        value = param[5] if len(param) > 5 else None

        if isinstance(var1, list):
            var1_list = var1
        else:
            var1_list = [var1]

        for index, row in df.iterrows():
            value1 = row.get(var1)
            for var1 in var1_list:
                value2 = row.get(var2)
                if update_function(df, index, var1, var2, value, errors, qcc, message):
                    errors.append({
                        'uuid': str(row['uuid']),
                        'fecha': row['fecha'],
                        'codigo': row['codigo'],
                        'fecha_proc': datetime.now(),
                        'qcc': int(qcc),
                        'variable_1': var1,
                        'value_1': value1,
                        'variable_2': var2,
                        'value_2': value2,
                        'value_fix': None  # Ajusta según la lógica de tu función de actualización
                    })

    return errors, df

def detect_locks(df, columns_to_check):
    errores = []
    for column, check in columns_to_check:
        df.sort_values(by=['codigo', 'fecha'], inplace=True)
        group_rolling = df.groupby('codigo')[column].rolling(window=check, min_periods=check)
        blocked_values = group_rolling.apply(lambda x: x.round(3).nunique() == 1, raw=False)
        blocked_indices = blocked_values[blocked_values == 1].index

        for group_index in blocked_indices:
            index = group_index[1]
            errores.append({
                'uuid': str(df.at[index, 'uuid']),
                'fecha': df.at[index, 'fecha'],
                'codigo': df.at[index, 'codigo'],
                'fecha_proc': datetime.now(),
                'qcc': 510,
                'variable_1': column,
                'value_1': df.at[index, column],
                'variable_2': None,
                'value_2': None,
                'value_fix': None
            })

            for col in df.columns:
                if col not in ['uuid', 'codigo', 'fecha']:
                    df.at[index, col] = None

    return errores, df