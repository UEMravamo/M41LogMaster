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

    def test_case5(self):
        """
        Quinta prueba unitaria para la función analisis_log.
        """
        input_file = "../input-file-10000.txt"
        init_datetime = "Martes, 13 de agosto de 2019 01:00:00"
        end_datetime = "Martes, 13 de agosto de 2019 21:00:00"
        target_host = "Tyreonna"

        expected_result = []
        result = analisis_log(input_file, init_datetime, end_datetime, target_host)
        self.assertEqual(result, expected_result)

    def test_case6(self):
        """
        Prueba con un rango de tiempo al borde de los registros existentes.
        """
        input_file = "../input-file-10000.txt"
        init_datetime = "Martes, 13 de agosto de 2019 01:00:00"
        end_datetime = "Martes, 13 de agosto de 2019 02:00:00"
        target_host = "Heera"

        expected_result = ["Reneisha"]
        result = analisis_log(input_file, init_datetime, end_datetime, target_host)
        self.assertEqual(result, expected_result)

    def test_case7(self):
        """
        Última prubea unitaria para la función analisis_log.
        """
        input_file = "../input-file-10000.txt"
        init_datetime = "Martes, 13 de agosto de 2019 01:00:00"
        end_datetime = "Martes, 13 de agosto de 2019 21:00:00"
        target_host = "Jeremyah"

        expected_result = ["Ahmira"]
        result = analisis_log(input_file, init_datetime, end_datetime, target_host)
        self.assertEqual(result, expected_result)


if __name__ == "__main__":
    archivo_logs = "input-file-10000.txt"  
    conexiones = 1                     
    intervalo_segundos = 2              

    generar_conexiones(archivo_logs, conexiones, intervalo_segundos)