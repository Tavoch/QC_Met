import os
import subprocess

# Ruta al script de Python
script_path = 'C:\\git\\Meteo_QC\\QC_Met\\QC_Met.py'

# Ejecutar el script de Python
subprocess.run(['python', script_path], check=True)

# Pausa para ver cualquier mensaje de error
os.system("pause")