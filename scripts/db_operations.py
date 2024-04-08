from sqlalchemy import text
from datetime import datetime


def truncate_tables(conn, tables):
    with conn.cursor() as cur:
        for table in tables:
            try:
                # Verificar si la tabla existe
                cur.execute(f"SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = '{table}');")
                if cur.fetchone()[0]:
                    cur.execute(f"TRUNCATE TABLE {table} CASCADE;")
                    conn.commit()  # Confirmar la transacción
                    print(f"Tabla {table} truncada exitosamente.")
                else:
                    print(f"Tabla {table} no existe.")
            except Exception as e:
                conn.rollback()  # Revertir la transacción en caso de error
                print(f"Error al truncar la tabla {table}: {e}")



def upsert_dataframe(df, table_name, engine, unique_columns):
    temp_table = f"{table_name}_temp"
    df.to_sql(temp_table, engine, if_exists='replace', index=False, method='multi', chunksize=1000)

    columns = df.columns.tolist()
    columns_str = ", ".join([f'"{col}"' for col in columns])
    update_columns_str = ", ".join([f'"{col}" = EXCLUDED."{col}"' for col in columns if col not in unique_columns])

    upsert_query = f"""
    INSERT INTO "{table_name}" ({columns_str})
    SELECT {columns_str}
    FROM "{temp_table}"
    ON CONFLICT ({", ".join(unique_columns)}) DO UPDATE SET
    {update_columns_str};
    """

    with engine.begin() as conn:
        conn.execute(text(upsert_query))
        conn.execute(text(f"DROP TABLE IF EXISTS {temp_table};"))


def insert_error(conn, error):
    with conn.cursor() as cur:
        error_uuid, fecha, qcc, codigo, variable_1, value_1, variable_2, value_2, value_fix = (
            error['uuid'],
            error['fecha'],
            error['qcc'],
            int(error['codigo']),
            error['variable_1'],
            error['value_1'],
            error['variable_2'],
            error['value_2'],
            error['value_fix']
        )
        cur.execute("""
            SELECT COUNT(*) FROM errores
            WHERE uuid = %s AND fecha = %s AND variable_1 = %s
        """, (error_uuid, fecha, variable_1))
        if cur.fetchone()[0] == 0:
            cur.execute("""
                INSERT INTO errores (uuid, fecha, codigo, fecha_proc, qcc, variable_1, value_1, variable_2, value_2, value_fix)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (error_uuid, fecha, codigo, datetime.now(), qcc, variable_1, value_1, variable_2, value_2, value_fix))
        else:
            cur.execute("""
                UPDATE errores
                SET fecha_proc = %s, qcc = %s, codigo=%s, value_1 = %s, value_2 = %s, value_fix = %s
                WHERE uuid = %s AND fecha = %s AND variable_1 = %s AND variable_2 = %s
            """, (datetime.now(), qcc, codigo, value_1, value_2, value_fix, error_uuid, fecha, variable_1, variable_2))
        conn.commit()

def update_processing_record(conn, codigo, nombre_archivo, lineas_procesadas, primera_fecha, ultima_fecha, fecha_proc):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT id, lineas_procesadas FROM registro_procesamiento
            WHERE codigo = %s AND nombre_archivo = %s
        """, (codigo, nombre_archivo))
        result = cur.fetchone()
        if result and result[1] < lineas_procesadas:
            cur.execute("""
                UPDATE registro_procesamiento
                SET lineas_procesadas = %s, fecha_inicial = %s, fecha_final = %s, fecha_proc = %s
                WHERE id = %s
                """, (lineas_procesadas, primera_fecha, ultima_fecha, fecha_proc, result[0]))
        elif not result:
            cur.execute("""
                INSERT INTO registro_procesamiento (codigo, nombre_archivo, lineas_procesadas, fecha_inicial, fecha_final, fecha_proc)
                VALUES (%s, %s, %s, %s, %s, %s);
            """, (codigo, nombre_archivo, lineas_procesadas, primera_fecha, ultima_fecha, fecha_proc))
        conn.commit()