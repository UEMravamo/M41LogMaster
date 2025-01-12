# Funciones y código del ejercicio 1 -> análisis logs
from pyspark.sql import functions as F
from datetime import datetime
import locale
from utils.utils import (
    crear_sesion_spark,
    definir_esquema,
    verificar_archivo,
    cargar_datos_csv
)


def configurar_locale():
    """
    Configura la localización del sistema para manejo de fechas en español.
    """
    try:
        locale.setlocale(locale.LC_TIME, 'es_ES.UTF-8')
    except locale.Error:
        locale.setlocale(locale.LC_TIME, 'Spanish_Spain.1252')


def convertir_fechas(init_datetime, end_datetime, input_format):
    """
    Convierte las fechas inicial y final al formato de timestamp en milisegundos.
    """
    init_dt = datetime.strptime(init_datetime, input_format)
    end_dt = datetime.strptime(end_datetime, input_format)
    return int(init_dt.timestamp() * 1000), int(end_dt.timestamp() * 1000)


def filtrar_datos_por_tiempo(df, init_timestamp, end_timestamp):
    """
    Filtra el DataFrame para incluir únicamente los registros dentro del rango de tiempo.
    """
    return df.filter(
        (F.col("timestamp") >= init_timestamp) &
        (F.col("timestamp") <= end_timestamp)
    )


def filtrar_por_hostname(df, hostname):
    """
    Filtra el DataFrame para incluir únicamente los registros donde el hostname
    de origen o destino coincide con el especificado.
    """
    target_host = hostname.lower()
    return df.filter(
        (F.lower(F.col("hostname_origen")) == target_host) |
        (F.lower(F.col("hostname_destino")) == target_host)
    )


def obtener_hosts_conectados(df, hostname):
    """
    Obtiene una lista de hosts conectados al hostname especificado.
    """
    target_host = hostname.lower()
    df_distinct_origin_host = df.filter(
        F.lower(F.col('hostname_origen')) != target_host
    ).select(F.col('hostname_origen').alias('host'))

    df_distinct_destino_host = df.filter(
        F.lower(F.col('hostname_destino')) != target_host
    ).select(F.col('hostname_destino').alias('host'))

    df_result = df_distinct_origin_host.unionByName(
        df_distinct_destino_host
    ).distinct()
    
    return [row['host'] for row in df_result.toLocalIterator()]


def analisis_log(input_file, init_datetime, end_datetime, hostname):
    """
    Realiza el análisis de logs filtrando por archivo, rango de tiempo y hostname.
    """
    verificar_archivo(input_file)

    spark = crear_sesion_spark("analisisLog", "4g", "4g", "200")
    schema = definir_esquema()
    df_log_csv = cargar_datos_csv(spark, input_file, schema)

    configurar_locale()

    input_format = "%A, %d de %B de %Y %H:%M:%S"
    init_timestamp, end_timestamp = convertir_fechas(
        init_datetime, end_datetime, input_format
    )

    df_filtered = filtrar_datos_por_tiempo(
        df_log_csv, init_timestamp, end_timestamp
    )
    df_filtered = filtrar_por_hostname(df_filtered, hostname)

    hosts = obtener_hosts_conectados(df_filtered, hostname)

    spark.stop()

    return hosts
