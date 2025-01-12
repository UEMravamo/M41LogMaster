import unittest
from analisis_log import analisis_log

def test_analisis_log():
    """
    Prueba unitaria para la función analisis_log.
    """
    # Parámetros de prueba
    input_file = '../input-file-10000.txt'
    init_datetime = 'Martes, 13 de agosto de 2019 01:00:00'
    end_datetime = 'Martes, 13 de agosto de 2019 21:00:00'
    target_host = 'Savhannah'

    # Resultado esperado
    expected_result = [
        'Deyshawn', 'Rumaldo', 'Jaylien', 'Zarriyah', 'Shaynee',
        'Demarius', 'Borna', 'Elmir', 'Michelene', 'Ajee', 'Tanisha'
    ]

    # Llamada a la función
    result = analisis_log(input_file, init_datetime, end_datetime, target_host)

    # Validación del resultado
    assert result == expected_result, f"Error: Resultado inesperado {result}"
