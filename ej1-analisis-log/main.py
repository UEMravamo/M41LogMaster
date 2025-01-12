# Main del ejercicio 1
import sys
import os

sys.path.append(os.path.dirname(os.path.abspath(__file__)) + '/../')
from analisis_log import analisis_log 


def main():
    """
    Punto de entrada principal del ejercicio 1.
    """
    input_file = '../input-file-10000.txt'
    init_datetime = 'Martes, 13 de agosto de 2019 01:00:00'
    end_datetime = 'Martes, 13 de agosto de 2019 21:00:00'
    target_host = 'Savhannah'

    result = analisis_log(input_file, init_datetime, end_datetime, target_host)

    print(result)


if __name__ == "__main__":
    main()
