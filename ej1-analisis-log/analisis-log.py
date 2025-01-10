# Funciones y código del ejercicio 1 -> análisis logs
import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, LongType, StringType
from datetime import datetime
import locale

def cargar_datos_csv(spark, input_file, schema):
    return spark.read.schema(schema).csv(input_file, sep=" ", header=False)

def configurar_locale():
    try:
        locale.setlocale(locale.LC_TIME, 'es_ES.UTF-8')
    except locale.Error:
        locale.setlocale(locale.LC_TIME, 'Spanish_Spain.1252')

def convertir_fechas(init_datetime, end_datetime, input_format):
    init_dt = datetime.strptime(init_datetime, input_format)
    end_dt = datetime.strptime(end_datetime, input_format)
    return int(init_dt.timestamp() * 1000), int(end_dt.timestamp() * 1000)

def filtrar_datos_por_tiempo(df, init_timestamp, end_timestamp):
    return df.filter(
        (F.col("timestamp") >= init_timestamp) &
        (F.col("timestamp") <= end_timestamp)
    )