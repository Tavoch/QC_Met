import psycopg2
from sqlalchemy import create_engine

# db_config.py
database_url = "dbname=qcmeteo user=postgres password=gherrera host=localhost"

def get_database_connection():
    # Establecer la conexión a la base de datos
    databaseurl = "postgresql://postgres:gherrera@localhost:5432/qcmeteo"
    conn = psycopg2.connect(databaseurl)
    engine = create_engine(databaseurl)
    return conn, engine