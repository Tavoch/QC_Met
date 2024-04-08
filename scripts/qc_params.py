# Definición de rangos
ranges = {
    #0-250m
    'rango_1': {
        'temp': {'temp_ai_min': {'min': 7, 'max': 44},'temp_ai_med': {'min': 7, 'max': 44}, 'temp_ai_max': {'min': 7, 'max': 44}}
    },
    #250-500
    'rango_2': {
        'temp': {'temp_ai_min': {'min': 5, 'max': 46},'temp_ai_med': {'min': 5, 'max': 46}, 'temp_ai_max': {'min': 5, 'max': 46}}
    },
    #500-1000m
    'rango_3': {
        'temp': {'temp_ai_min': {'min': 2, 'max': 45},'temp_ai_med': {'min': 2, 'max': 45},'temp_ai_max': {'min': 2, 'max': 45}}
    },
    #1000-2000m
    'rango_4': {
       'temp': {'temp_ai_min': {'min': -2, 'max': 42},'temp_ai_med': {'min': -2, 'max': 42},'temp_ai_max': {'min': -2, 'max': 42}}
    },
    #Más de 2000m
    'rango_5': {
        'temp': {'temp_ai_min': {'min': -25, 'max': 35},'temp_ai_med': {'min': -25, 'max': 35},'temp_ai_max': {'min': -25, 'max': 35}}
    },
    # Resto
    'rango_6': {
        'humedad': {'hum_rel_med': {'min': 0, 'max': 100}},
        'Velocidad_viento': {'vviento_max': {'min': 0, 'max': 75},'vviento_med': {'min': 0, 'max': 75}},#,'vviento_min': {'min': 0, 'max': 75}},
        'Direccion_viento': {'dviento_max': {'min': 0, 'max': 360},'dviento_med': {'min': 0, 'max': 360}},#,'dviento_sig': {'min': 0, 'max': 360}},
        'Presion':{'presion_med': {'min': 700, 'max': 1080}},
        'Bateria':{'bateria_max': {'min': 12, 'max': 14}},
    },
    'rango_7': {
        'Precipitacion': {'lluvia_acu': {'min': 0, 'max': 50}},
    },
    'rango_8': {
        'Precipitacion': {'lluvia_acu': {'min': 0, 'max': 50},'lluvia1_acu': {'min': 0, 'max': 50},'lluvia2_acu': {'min': 0, 'max': 50}},
    },
    'rango_9': {
        'Precipitacion': {'lluvia2_acu': {'min': 0, 'max': 50}},
    },
    'rango_10': {
        'Radiacion':{'rglobal1_max': {'min': -1, 'max': 1350},'rglobal1_med': {'min': -1, 'max': 1350}},
    },
}

# Grupos de carpetas con sus respectivos rangos
group_folder = {
    'grupo_1': {
        'carpetas': ['4162'],
        'rangos': ['rango_1', 'rango_6', 'rango_9', 'rango_10']
    },
    'grupo_2': {
        'carpetas': ['4167','3724','4150','4159','4164','4166'],
        'rangos': ['rango_1', 'rango_6', 'rango_7','rango_10']
    },
    'grupo_3': {
        'carpetas': ['4151','4159'],
        'rangos': ['rango_1', 'rango_6', 'rango_7']
    },
    'grupo_4': {
        'carpetas': ['3700'],
        'rangos': ['rango_1', 'rango_6', 'rango_7']
    },
    'grupo_5': {
        'carpetas': ['4152','4158'],
        'rangos': ['rango_3', 'rango_6', 'rango_7']
    },
    'grupo_6': {
        'carpetas': ['3693','3696','3694','3709'],
        'rangos': ['rango_3', 'rango_6', 'rango_9']
    },
    'grupo_7': {
        'carpetas': ['3713'],
        'rangos': ['rango_3', 'rango_6', 'rango_7']
    },
    'grupo_8': {
        'carpetas': ['3978','3722'],
        'rangos': ['rango_4', 'rango_6', 'rango_7', 'rango_10']
    },
    'grupo_9': {
        'carpetas': ['4150'],
        'rangos': ['rango_4', 'rango_6', 'rango_7']
    },
    # Agrega más grupos según sea necesario
}

# Diccionario para almacenar la asignación de carpetas y rangos
folders_ranges = {}

# Asignar los rangos a cada carpeta en cada grupo
for group in group_folder.values():
    for folder in group['carpetas']:
        folders_ranges[folder] = group['rangos']


variables_and_ranges = {
    'temp_ai_med': {'qcc': 540, 'limite_atras': 6, 'limite_adelante': 0, 'umbral_cambio': 4},
    'temp_ai_min': {'qcc': 540, 'limite_atras': 6, 'limite_adelante': 0, 'umbral_cambio': 4},
    'temp_ai_max': {'qcc': 540, 'limite_atras': 6, 'limite_adelante': 0, 'umbral_cambio': 4},
    'vviento_max': {'qcc':540, 'limite_atras': 1, 'limite_adelante': 1, 'umbral_cambio': 20},
    # Agrega más variables según sea necesario
}

# Asignación de rangos de temperatura por grupo de carpetas
ranges_v = {}
for codigo, ranges_list in folders_ranges.items():
    ranges_v[codigo] = []
    for range_l in ranges_list:
        ranges_v[codigo].append(ranges[range_l])

# Imprimir el diccionario de rangos asignados para verificar
#print("Asignación de rangos a códigos de carpeta:")
#for codigo, rango in rangos_v.items():
        #print(f"  Código: {codigo}, Rango: {rango}")

# Configuración QC Bloqueos
columns_to_check = [
    ('dviento_med', 2),
]

# QC rain check
station_mapping = {
        #'3713': {'aemet': '', 'agrocabildo': [58, 69, 70], 'campos_lluvia': ['lluvia_acu']},
        #'4162': {'aemet': '', 'agrocabildo': [13,201], 'campos_lluvia': ['lluvia_acu', 'lluvia1_acu', 'lluvia2_acu']},
    '4152': {'aemet': 'C419X', 'agrocabildo':[75,76], 'campos_lluvia': ['lluvia_acu']},
    '4160': {'aemet': 'C429I', 'agrocabildo':[92,2], 'campos_lluvia': ['lluvia_acu']},
    '4164': {'aemet': 'C429I', 'agrocabildo':[92,2], 'campos_lluvia': ['lluvia_acu']},
    '4167': {'aemet': '', 'agrocabildo':[102,65], 'campos_lluvia': ['lluvia_acu']},
    '3724': {'aemet': 'C438N', 'agrocabildo':[86,96], 'campos_lluvia': ['lluvia_acu']},
    '3709': {'aemet': 'C468X', 'agrocabildo':[73,91], 'campos_lluvia': ['lluvia2_acu']},
    '4150': {'aemet': '', 'agrocabildo':[87,57], 'campos_lluvia': ['lluvia_acu']},
    '3713': {'aemet': 'C406G', 'agrocabildo':[69,70,58], 'campos_lluvia': ['lluvia_acu']},
    '4158': {'aemet': 'C446G', 'agrocabildo':[86,190], 'campos_lluvia': ['lluvia_acu']},
    '3700': {'aemet': 'C449C', 'agrocabildo':[86,190], 'campos_lluvia': ['lluvia_acu']},
    '4166': {'aemet': 'C458A', 'agrocabildo':[11,199], 'campos_lluvia': ['lluvia_acu']},
    '3696': {'aemet': 'C446G', 'agrocabildo':[105], 'campos_lluvia': ['lluvia2_acu']},
    '4162': {'aemet': 'C449C', 'agrocabildo':[92], 'campos_lluvia': ['lluvia_acu', 'lluvia1_acu', 'lluvia2_acu']},
    '4151': {'aemet': 'C449F', 'agrocabildo':[92,101], 'campos_lluvia': ['lluvia_acu']},
    '4159': {'aemet': '', 'agrocabildo':[95,93], 'campos_lluvia': ['lluvia_acu']},
    '3694': {'aemet': '', 'agrocabildo':[64,74], 'campos_lluvia': ['lluvia2_acu']},
    '3978': {'aemet': '', 'agrocabildo':[13,201], 'campos_lluvia': ['lluvia_acu']},

}

