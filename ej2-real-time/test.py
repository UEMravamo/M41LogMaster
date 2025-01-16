import time
import random
import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)) + '/../')
from utils.utils import verificar_archivo

HOSTNAMES = ["Alpha", "Beta", "Gamma", "Delta", "Savhannah", "Zeta", "Epsilon", "Theta"]

def generar_conexiones(file_path, num_conexiones=10, intervalo=1):
    """
    Genera un archivo de logs con conexiones ficticias.

    Args:
        file_path (str): Ruta del archivo donde se guardarán los registros.
        num_conexiones (int): Número de conexiones a generar.
        intervalo (int): Intervalo en segundos entre cada conexión generada.
    """
    print(f"Generando {num_conexiones} conexiones ficticias en '{file_path}'...")

    verificar_archivo(file_path)

    with open(file_path, 'a') as log_file:
        for _ in range(num_conexiones):
            timestamp = int(time.time() * 1000)

            origen = random.choice(HOSTNAMES)
            destino = "Prueba"

            log_line = f"\n{timestamp} {origen} {destino}"

            log_file.write(log_line)

            print(f"Conexión generada: {log_line.strip()}")

            time.sleep(intervalo)


if __name__ == "__main__":
    archivo_logs = "input-file-10000.txt"  
    conexiones = 1                     
    intervalo_segundos = 2              

    generar_conexiones(archivo_logs, conexiones, intervalo_segundos)