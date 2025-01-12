# Funciones y código del ejercicio 2 -> real-time
import os
import time
from datetime import datetime, timedelta
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from pyspark.sql import functions as F
from utils.utils import crear_sesion_spark, definir_esquema, verificar_archivo, cargar_datos_csv

class LogFileHandler(FileSystemEventHandler):
    def __init__(self, df_log, hostname):
        self.df_log = df_log
        self.hostname = hostname.lower()
        self.last_timestamp = int((datetime.now() - timedelta(hours=1)).timestamp() * 1000)

    def process_logs(self):

        df_last_hour = self.df_log.filter(F.col("timestamp") >= self.last_timestamp)
        df_last_hour.show()
        connected_to_host = df_last_hour.filter(
            F.lower(F.col("hostname_destino")) == self.hostname
        ).select("hostname_origen").distinct()

        connected_from_host = df_last_hour.filter(
            F.lower(F.col("hostname_origen")) == self.hostname
        ).select("hostname_destino").distinct()

        most_connections = df_last_hour.groupBy("hostname_origen").count().orderBy(
            F.col("count").desc()
        ).limit(1)

        print("\n--- Resultados de la última hora ---")
        print("Hostnames conectados al host configurado:")
        connected_to_host.show()

        print("Hostnames que recibieron conexiones del host configurado:")
        connected_from_host.show()

        print("Hostname con más conexiones en la última hora:")
        most_connections.show()

        self.last_timestamp = int((datetime.now() - timedelta(hours=1)).timestamp() * 1000)

    def on_modified(self, event):
        if event.src_path == self.input_file:
            print(f"Archivo modificado: {event.src_path}")
            self.process_logs()

def monitor_log_file(input_file, df_log, hostname):
    event_handler = LogFileHandler(df_log, hostname)
    observer = Observer()
    observer.schedule(event_handler, path=os.path.dirname(input_file), recursive=False)
    observer.start()
    print(f"Monitorizando cambios en {input_file}...")
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
    observer.join()

def real_time(input_file, hostname):
    verificar_archivo(input_file)

    spark = crear_sesion_spark("realtime", "4g", "4g", "200")
    schema = definir_esquema()
    df_log_csv = cargar_datos_csv(spark, input_file, schema)
    monitor_log_file(input_file, df_log_csv, hostname)

    spark.stop()