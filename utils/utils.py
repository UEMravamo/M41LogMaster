#Archivo de funciones comunes a los dos ejercicios
import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, LongType, StringType

def crear_sesion_spark(app_name, memoria_ejecutor, memoria_driver, particiones_shuffle):
    return SparkSession.builder.appName(app_name).config(
        "spark.executor.memory", memoria_ejecutor
    ).config(
        "spark.driver.memory", memoria_driver
    ).config(
        "spark.sql.shuffle.partitions", particiones_shuffle
    ).getOrCreate()

def definir_esquema():
    return StructType([
        StructField("timestamp", LongType(), True),
        StructField("hostname_origen", StringType(), True),
        StructField("hostname_destino", StringType(), True)
    ])

def verificar_archivo(input_file):
    if not os.path.exists(input_file):
        print(f'El archivo introducido: {input_file} no existe')
        sys.exit(1)