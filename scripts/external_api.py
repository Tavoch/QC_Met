import requests
import io
import pandas as pd
from datetime import datetime, timedelta
from scripts.config import *
from utils import create_unique_id

def get_rain_sensor_code_agrocabildo(station, headers):
    url = f"https://datos.tenerife.es/api/meteo/latest/stations/{station}/sensors"
    print (url)
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        data = response.json()
        if 'stations' in data and data['stations']:
            sensors = data['stations'][0]['sensors']
            for sensor in sensors:
                if sensor['sensor_alias'] == 'RAIN':
                    return sensor['id_weatherstationsensor']
        else:
            print(f"La estación {station} no tiene sensores o la clave 'stations' no se encuentra en la respuesta.")
    else:
        print(f"Error al obtener el código del sensor de lluvia para la estación {station}: ", response.status_code)
    return None

if filter_by_interval:
    start_date = start_date
    end_date = end_date
else:
    # Define la fecha de inicio si se filtra por días atrás
    days_back = days_back
    start_date = datetime.now() - timedelta(days=days_back)
    end_date = datetime.now()

def get_agrocabildo_api_data(url_base, stations_agrocabildo, headers):
    start_date_str = start_date.strftime('%Y-%m-%d')
    end_date_str = end_date.strftime('%Y-%m-%d')

    dfs = []

    for station in stations_agrocabildo:
        sensor_lluvia = get_rain_sensor_code_agrocabildo(station, headers)
        if sensor_lluvia is None:
            continue

        url = f"{url_base}/station/{station}/sensor/{sensor_lluvia}/from/{start_date_str}/to/{end_date_str}/1"
        print (url)
        try:
            response = requests.get(url, headers=headers)

            if response.status_code == 200:
                readings = response.json()['readings']
                sensor_data = readings['sensors'][0]['values']
                df = pd.DataFrame(sensor_data)
                df['observation_date'] = pd.to_datetime(df['observation_date'])
                df['codigo'] = station
                df['uuid'] = df.apply(lambda row: create_unique_id(row['observation_date'], row['codigo']), axis=1)
                df.rename(columns={'observation_date': 'fecha', 'observation_value': 'lluvia_acu'}, inplace=True)
                df = df[['uuid', 'codigo', 'fecha', 'lluvia_acu']]
                dfs.append(df)
            else:
                print(f"Error en la solicitud para la estación {station}: ", response.status_code)
        except Exception as e:
            print(f"Error al procesar la estación {station}: {e}")

    df_agrocabildo = pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()
    df_agrocabildo['lluvia_acu'] = pd.to_numeric(df_agrocabildo['lluvia_acu'], errors='coerce')
    df_agrocabildo.to_csv("external\\datos_agrocabildo.csv", index=False)
    return df_agrocabildo


def get_aemet_api_data(stations_aemet, api_key, url_aemet):
    dfs = []
    headers = {
        "accept": "application/json",
        "api_key": api_key
    }
    for station in stations_aemet:
        url = f"{url_aemet}/datos/estacion/{station}"
        print(url)
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            data_url = response.json()["datos"]
            data_response = requests.get(data_url)
            json_io = io.StringIO(data_response.text)
            df = pd.read_json(json_io)
            df['uuid'] = df.apply(lambda row: create_unique_id(row['fint'], row['idema']), axis=1)
            df.rename(columns={'idema': 'codigo', 'fint': 'fecha', 'prec': 'lluvia_acu'}, inplace=True)
            df = df[['uuid', 'codigo', 'fecha', 'lluvia_acu']]
            df['fecha'] = pd.to_datetime(df['fecha'])
            dfs.append(df)
        else:
            print(f"Error en la solicitud para la estación {station}: ", response.status_code)
    df_aemet = pd.concat(dfs, ignore_index=True)
    df_aemet.to_csv("external\\datos_aemet.csv", index=False)
    return df_aemet