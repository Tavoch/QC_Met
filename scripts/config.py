import psycopg2
from sqlalchemy import create_engine
from datetime import datetime

# Configuración de la ruta de la carpeta principal
main_folder_path = r'c:\\temp\\datos'

# Carpetas a procesar, [codigo]
stations = ['3694', '3696', '3700', '3709', '3713', '3978', '3724', '4150', '4151', '4152', '4158', '4159', '4162', '4164', '4166', '4167']

# Configuración de filtro
filter_by_interval = False
days_back = 1
# Descomentar y configurar si se usa filtro por intervalo
start_date = datetime.strptime('2023-10-17', '%Y-%m-%d')
end_date = datetime.strptime('2023-10-17', '%Y-%m-%d')

#API Aemet
stations_aemet = ["C419X", "C429I", "C438N", "C468X", "C406G", "C446G", "C458A", "C449C", "C449F"]
api_key = "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJ0YXZvLmhlcnJlcmFAZ21haWwuY29tIiwianRpIjoiMjdiYWY3MmUtMjY5NC00MGNlLWFmY2EtNjgzM2E1OGU1ZDdlIiwiaXNzIjoiQUVNRVQiLCJpYXQiOjE2OTY0MDM4MzUsInVzZXJJZCI6IjI3YmFmNzJlLTI2OTQtNDBjZS1hZmNhLTY4MzNhNThlNWQ3ZSIsInJvbGUiOiIifQ.qRFihZZUeXdYl-rMW5nVpteCsLznngtnCfIVBIOAsPE"
url_aemet = "https://opendata.aemet.es/opendata/api/observacion/convencional"

# API Agrocabildo
url_agrocabildo = "https://datos.tenerife.es/api/meteo/latest/readings"
stations_agrocabildo = [11, 13, 57, 58, 64, 65, 69, 70, 73, 74, 75, 76, 86, 87, 91, 92, 93, 95, 96, 101, 102, 105, 190, 199, 201]
headers = {
    "accept": "application/json"
}

# db_config.py
database_url = "dbname=qcmeteo user=postgres password=gherrera host=localhost"

def get_database_connection():
    # Establecer la conexión a la base de datos
    databaseurl = "postgresql://postgres:gherrera@localhost:5432/qcmeteo"
    conn = psycopg2.connect(databaseurl)
    engine = create_engine(databaseurl)
    return conn, engine

