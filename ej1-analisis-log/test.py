# Pruebas unitarias ej 1
import unittest
import os
from pyspark.sql import SparkSession
from analisisLog import analisisLog

class TestAnalisisLogRealFile(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.master("local[1]").appName("TestAnalisisLogRealFile").getOrCreate()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_case_1(self):
        self._run_test("Aadvik", 1565647204351, 1565647246869, {"Matina"})

    def test_case_2(self):
        self._run_test("Matina", 1565647246870, 1565647291234, {"Aadvik", "Ximena"})

    def test_case_3(self):
        self._run_test("Ximena", 1565647291235, 1565647345678, {"Matina"})

    def test_case_4(self):
        self._run_test("Aadvik", 1565647204351, 1565647345678, {"Matina", "Ximena"})

    def test_case_5(self):
        self._run_test("Host_Alpha", 1565647000000, 1565648000000, {"Host_Beta"})

    def test_case_6(self):
        self._run_test("Host_Beta", 1565647000000, 1565648000000, {"Host_Alpha", "Host_Gamma"})

    def test_case_7(self):
        self._run_test("Host_Gamma", 1565647000000, 1565648000000, {"Host_Beta"})

    def test_case_8(self):
        self._run_test("Host_Delta", 1565647500000, 1565648500000, {"Host_Epsilon"})

    def test_case_9(self):
        self._run_test("Host_Epsilon", 1565647500000, 1565648500000, {"Host_Delta"})

    def test_case_10(self):
        self._run_test("Aadvik", 1565647204351, 1565647204352, {"Matina"})

    def test_case_11(self):
        self._run_test("Host_Alpha", 1565648000000, 1565649000000, {"Host_Beta"})

    def test_case_12(self):
        self._run_test("Host_Gamma", 1565648000000, 1565649000000, {"Host_Beta", "Host_Delta"})

    def test_case_13(self):
        self._run_test("Ximena", 1565647204351, 1565647291234, {"Matina"})

    def test_case_14(self):
        self._run_test("Aadvik", 1565647204351, 1565647291234, {"Matina"})

    def test_case_15(self):
        self._run_test("Host_Epsilon", 1565647500000, 1565648000000, {"Host_Delta"})

    def test_case_16(self):
        self._run_test("Host_Delta", 1565647500000, 1565648000000, {"Host_Epsilon"})

    def test_case_17(self):
        self._run_test("Host_Alpha", 1565647000000, 1565647500000, {"Host_Beta"})

    def test_case_18(self):
        self._run_test("Host_Beta", 1565647000000, 1565647500000, {"Host_Alpha"})

    def test_case_19(self):
        self._run_test("Ximena", 1565647204351, 1565647345678, {"Matina"})

    def test_case_20(self):
        self._run_test("Host_Delta", 1565647000000, 1565648000000, {"Host_Epsilon"})

    def test_case_21(self):
        self._run_test("Aadvik", 1565647204351, 1565648000000, {"Matina"})

    def test_case_22(self):
        self._run_test("Matina", 1565647204351, 1565648000000, {"Aadvik"})

    def test_case_23(self):
        self._run_test("Host_Alpha", 1565647500000, 1565648500000, {"Host_Beta"})

    def test_case_24(self):
        self._run_test("Host_Gamma", 1565647500000, 1565648500000, {"Host_Delta"})

    def test_case_25(self):
        self._run_test("Host_Delta", 1565647500000, 1565648500000, {"Host_Epsilon"})

    def test_case_26(self):
        self._run_test("Host_Epsilon", 1565647500000, 1565648500000, {"Host_Delta"})

    def test_case_27(self):
        self._run_test("Matina", 1565647204351, 1565647291234, {"Aadvik"})

    def test_case_28(self):
        self._run_test("Host_Alpha", 1565647000000, 1565648500000, {"Host_Beta"})

    def test_case_29(self):
        self._run_test("Host_Beta", 1565647000000, 1565648500000, {"Host_Alpha", "Host_Gamma"})

    def test_case_30(self):
        self._run_test("Host_Gamma", 1565647000000, 1565648500000, {"Host_Beta"})

    def _run_test(self, hostname, start_time, end_time, expected_hosts):
        input_file = "./input-file-10000.txt"
        if not os.path.exists(input_file):
            self.skipTest(f"El archivo {input_file} no estÃ¡ disponible para la prueba.")

        result = analisisLog(
            inputFile=input_file,
            init_datetime=start_time,
            end_datetime=end_time,
            hostname=hostname
        )

        result_hosts = {row['host'] for row in result}
        self.assertEqual(result_hosts, expected_hosts)

if __name__ == "__main__":
    unittest.main()