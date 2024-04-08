# file_operations.py
import subprocess

def run_robocopy(source, destination, max_age=7):
    result = subprocess.run(['ROBOCOPY', '/MIR', '/R:0', source, destination, f'/MAXAGE:{max_age}'], capture_output=True, text=True)
    print(result.stdout)