from db_tabledefinitions import fields_errors, executed_controls_fields, field_types, aemet_agrocabildo_field_types, processing_record_fields_types
import psycopg2
from db_config import database_url,get_database_connection


def create_tables_and_indices(conn):
    table_names = ['datos_meteo', 'datos_meteo_fix']

    with conn.cursor() as cur:
        for table_name in table_names:
            fields = [f"{field} {type}" for field, type in field_types.items()]
            query_create_table = f"CREATE TABLE IF NOT EXISTS {table_name} ({', '.join(fields)})"
            cur.execute(query_create_table)

            # Crear índices únicos para las columnas 'codigo' y 'fecha'
            if table_name == 'datos_meteo':
                cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_codigo_fecha ON datos_meteo (codigo, fecha)")
            elif table_name == 'datos_meteo_fix':
                cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_codigo_fecha_fix ON datos_meteo_fix (codigo, fecha)")

        # Creación de la tabla registro_procesamiento
        fields = [f"{field} {type}" for field, type in processing_record_fields_types.items()]
        query_create_table = f"CREATE TABLE IF NOT EXISTS registro_procesamiento ({', '.join(fields)})"
        cur.execute(query_create_table)

        # Confirmar todas las operaciones
        conn.commit()
        print("Tablas y índices creados exitosamente.")

def create_aemet_agrocabildo_tables_and_indices(conn):
    table_names = ['datos_aemet', 'datos_agrocabildo']

    with conn.cursor() as cur:
        for table_name in table_names:
            fields = [f"{field} {type}" for field, type in aemet_agrocabildo_field_types.items()]
            query_create_table = f"CREATE TABLE IF NOT EXISTS {table_name} ({', '.join(fields)})"
            cur.execute(query_create_table)

            # Crear índices únicos para las columnas 'codigo' y 'fecha'
            if table_name == 'datos_aemet':
                cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_codigo_fecha_aemet ON datos_aemet (codigo, fecha)")
            elif table_name == 'datos_agrocabildo':
                cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_codigo_fecha_agrocabildo ON datos_agrocabildo (codigo, fecha)")

        # Confirmar todas las operaciones
        conn.commit()
        print("Tablas y índices de AEMET y Agrocabildo creados exitosamente.")

def create_tables(conn):
    with conn.cursor() as cur:
        # Crear tabla de errores
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS errores (
                {', '.join([f'{k} {v}' for k, v in fields_errors.items()])}
            );
        """)

        # Crear tabla de controles exitosos
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS controles_ejecutados (
                {', '.join([f'{k} {v}' for k, v in executed_controls_fields.items()])}
            );
        """)

        # Añade más comandos SQL para crear otras tablas según sea necesario

        # Confirmar cambios
        conn.commit()

if __name__ == '__main__':
    # Conectar a la base de datos
    with psycopg2.connect(get_database_connection) as conn:
        create_tables(conn)
