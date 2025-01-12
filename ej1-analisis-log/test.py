import unittest
import sys
import os

sys.path.append(os.path.dirname(os.path.abspath(__file__)) + '/../')
from analisis_log import analisis_log 


class TestAnalisisLog(unittest.TestCase):
    """
    Clase de prueba para la función analisis_log.
    """

    def test_analisis_log(self):
        """
        Primera prueba unitaria para la función analisis_log.
        """

        input_file = '../input-file-10000.txt'
        init_datetime = 'Martes, 13 de agosto de 2019 01:00:00'
        end_datetime = 'Martes, 13 de agosto de 2019 21:00:00'
        target_host = 'Savhannah'

        expected_result = [
            'Deyshawn', 'Rumaldo', 'Jaylien', 'Zarriyah', 'Shaynee',
            'Demarius', 'Borna', 'Elmir', 'Michelene', 'Ajee', 'Tanisha'
        ]

        result = analisis_log(input_file, init_datetime, end_datetime, target_host)

        self.assertEqual(result, expected_result)

    def test_case2(self):
        """
        Segunda prueba unitaria para la función analisis_log.
        """
        input_file = "../input-file-10000.txt"
        init_datetime = "Martes, 13 de agosto de 2019 10:00:00"
        end_datetime = "Martes, 13 de agosto de 2019 12:00:00"
        target_host = "Keimy"

        expected_result = []
        result = analisis_log(input_file, init_datetime, end_datetime, target_host)
        self.assertEqual(result, expected_result)

    def test_case3(self):
        """
        Prueba con un hostname no presente en los datos.
        """
        input_file = "../input-file-10000.txt"
        init_datetime = "Martes, 13 de agosto de 2019 01:00:00"
        end_datetime = "Martes, 13 de agosto de 2019 21:00:00"
        target_host = "Inexistente"

        expected_result = []
        result = analisis_log(input_file, init_datetime, end_datetime, target_host)
        self.assertEqual(result, expected_result)

    def test_case4(self):
        """
        Prueba con un rango de tiempo que no incluye registros.
        """
        input_file = "../input-file-10000.txt"
        init_datetime = "Lunes, 12 de agosto de 2019 01:00:00"
        end_datetime = "Lunes, 12 de agosto de 2019 21:00:00"
        target_host = "Aadvik"

        expected_result = []
        result = analisis_log(input_file, init_datetime, end_datetime, target_host)
        self.assertEqual(result, expected_result)
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
    unittest.main()
