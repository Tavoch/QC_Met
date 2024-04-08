import pandas as pd
import matplotlib.pyplot as plt

def analyze_data_by_station(df):
    station_descrip = df.groupby('codigo').describe()
    print("Análisis descriptivo por estación:")
    print(station_descrip)

    for station, group in df.groupby('codigo'):
        print(f"\nEstación: {station}")
        for column in group.select_dtypes(include=['float64', 'int64']).columns:
            if column == 'codigo':
                continue
            q1 = group[column].quantile(0.25)
            q3 = group[column].quantile(0.75)
            i_q_r = q3 - q1
            lower_threshold = q1 - 1.5 * i_q_r
            upper_threshold = q3 + 1.5 * i_q_r
            outliers = group[(group[column] < lower_threshold) | (group[column] > upper_threshold)]
            print(f"\nOutliers en {column}:")
            print(outliers)

def visual_summary_by_station(df):
    summary_by_station = df.groupby('codigo').agg({
        'temp_ai_med': ['mean', 'min', 'max', 'std'],
        'temp_ai_min': ['mean', 'min', 'max', 'std'],
        'temp_ai_max': ['mean', 'min', 'max', 'std'],
        'bateria_med': ['mean', 'min', 'max', 'std'],
        'bateria_min': ['mean', 'min', 'max', 'std'],
        'bateria_max': ['mean', 'min', 'max', 'std'],
        # Añade más variables según sea necesario
    })
    print("Resumen estadístico por estación:")
    print(summary_by_station)

    variables = ['temp_ai_med', 'bateria_med','temp_ai_min', 'bateria_min','temp_ai_max', 'bateria_med']
    for variable in variables:
        plt.figure(figsize=(12, 8))
        boxplot = df.boxplot(column=variable, by='codigo', grid=False)
        plt.title(f"Gráfico de caja de {variable} por estación")
        plt.ylabel(variable)
        plt.xlabel('Estación')
        plt.xticks(rotation=45)
        plt.show()