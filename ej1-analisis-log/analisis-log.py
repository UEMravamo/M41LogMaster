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

def filtrar_por_hostname(df, hostname):
    target_host = hostname.lower()
    return df.filter(
        (F.lower(F.col("hostname_origen")) == target_host) |
        (F.lower(F.col("hostname_destino")) == target_host)
    )

def obtener_hosts_conectados(df, hostname):
    target_host = hostname.lower()
    df_distinct_origin_host = df.filter(
        F.lower(F.col('hostname_origen')) != target_host
    ).select(F.col('hostname_origen').alias('host'))

    df_distinct_destino_host = df.filter(
        F.lower(F.col('hostname_destino')) != target_host
    ).select(F.col('hostname_destino').alias('host'))

    df_result = df_distinct_origin_host.unionByName(df_distinct_destino_host).distinct()
    return [row['host'] for row in df_result.toLocalIterator()]