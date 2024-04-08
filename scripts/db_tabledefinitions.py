# Definición de campos y tipos de datos
field_types = {
    "fecha": "TIMESTAMP",
    "fecha_proc": "TIMESTAMP",
    "uuid": "TEXT",
    "codigo": "INTEGER",
    "bateria_max": "NUMERIC(7,3)",
    "bateria_med": "NUMERIC(7,3)",
    "bateria_min": "NUMERIC(7,3)",
    "dviento_max": "NUMERIC(7,3)",
    "dviento_med": "NUMERIC(7,3)",
    "dviento_sig": "NUMERIC(7,3)",
    "estado_ins": "INTEGER",
    "hum_rel_med": "NUMERIC(7,3)",
    "intprecip_med": "NUMERIC(7,3)",
    "lluvia_acu": "NUMERIC(7,3)",
    "lluvia1_acu": "NUMERIC(7,3)",
    "lluvia2_acu": "NUMERIC(7,3)",
    "lluvia_ins": "NUMERIC(7,3)",
    "lluvia_max": "NUMERIC(7,3)",
    "presion_med": "NUMERIC(7,3)",
    "procio_med": "NUMERIC(7,3)",
    "rglobal1_max": "NUMERIC(7,3)",
    "rglobal1_med": "NUMERIC(7,3)",
    "temp_ai_max": "NUMERIC(7,3)",
    "temp_ai_med": "NUMERIC(7,3)",
    "temp_ai_min": "NUMERIC(7,3)",
    "vviento_max": "NUMERIC(7,3)",
    "vviento_med": "NUMERIC(7,3)",
    "vviento_min": "NUMERIC(7,3)",
    "vviento_inc": "NUMERIC(7,3)",
    "vviento_ins": "NUMERIC(7,3)",
    "vviento_int": "NUMERIC(7,3)",
    "vviento_sig": "NUMERIC(7,3)"
}

aemet_agrocabildo_field_types = {
    "uuid": "VARCHAR PRIMARY KEY",
    "fecha": "TIMESTAMP",
    "codigo": "VARCHAR",
    "lluvia_acu": "NUMERIC(7,3)"
}

processing_record_fields_types = {
    "id": "SERIAL PRIMARY KEY",
    "codigo": "INTEGER",
    "nombre_archivo": "VARCHAR(255)",
    "lineas_procesadas": "INT",
    "fecha_inicial": "TIMESTAMP",
    "fecha_final": "TIMESTAMP",
    "fecha_proc": "TIMESTAMP"
}

# Procesos diezminutales
rain_columns = ['lluvia_acu', 'lluvia1_acu', 'lluvia2_acu', 'lluvia_max','intprecip_med']

average_columns = [
    'fecha_proc',
    'uuid',
    'codigo',
    'bateria_max',
    'bateria_med',
    'bateria_min',
    'dviento_max',
    'dviento_med',
    'dviento_sig',
    'hum_rel_med',
    'estado_ins',
    'intprecip_med',
    'lluvia_acu',
    'lluvia1_acu',
    'lluvia2_acu',
    'lluvia_ins',
    'lluvia_max',
    'presion_med',
    'procio_med',
    'rglobal1_max',
    'rglobal1_med',
    'temp_ai_max',
    'temp_ai_med',
    'temp_ai_min',
    'vviento_max',
    'vviento_med',
    'vviento_min',
    'vviento_inc',
    'vviento_ins',
    'vviento_int',
    'vviento_sig',
    # Añade más columnas según sea necesario
]

fields_errors = {
    "uuid": "VARCHAR (255) NOT NULL",
    "fecha": "TIMESTAMP NOT NULL",
    "codigo": "INTEGER",
    "fecha_proc": "TIMESTAMP NOT NULL",
    "qcc": "INTEGER NOT NULL",
    "variable_1": "VARCHAR (255)",
    "value_1": "NUMERIC (7,3)",
    "variable_2": "VARCHAR (255)",
    "value_2": "NUMERIC (7,3)",
    "value_fix": "NUMERIC (7,3)",
    "id": "serial PRIMARY KEY"
}

executed_controls_fields = {
    "uuid": "VARCHAR (255) PRIMARY KEY",
    "qcc": "INTEGER NOT NULL",
    "fecha_proc": "TIMESTAMP NOT NULL"
}