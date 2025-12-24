from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import sys

class SparkStreamingProcessor:
    def __init__(self, app_name="IoT-Streaming-Processor"):
        """Initialise la session Spark"""
        try:
            self.spark = SparkSession.builder \
                .appName(app_name) \
                .config("spark.jars.packages", 
                       "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
                .config("spark.sql.streaming.checkpointLocation", "/tmp/checkpoint") \
                .getOrCreate()
            
            self.spark.sparkContext.setLogLevel("WARN")
            print(f"Spark Session initialisee: {app_name}")
            print(f"Spark Version: {self.spark.version}")
            print("-" * 70)
        except Exception as e:
            print(f"Erreur lors de l'initialisation de Spark: {e}")
            sys.exit(1)
    
    def define_schema(self):
        """Définit le schéma des données IoT"""
        schema = StructType([
            StructField("sensor_id", StringType(), True),
            StructField("sensor_type", StringType(), True),
            StructField("location", StringType(), True),
            StructField("value", DoubleType(), True),
            StructField("unit", StringType(), True),
            StructField("timestamp", StringType(), True),
            StructField("is_anomaly", BooleanType(), True),
            StructField("battery_level", DoubleType(), True)
        ])
        return schema
    
    def read_from_kafka(self, kafka_servers="kafka:9092", topic="iot-sensors"):
        """Lit les données depuis Kafka"""
        print(f"\nLecture depuis Kafka...")
        print(f"  Serveurs: {kafka_servers}")
        print(f"  Topic: {topic}")
        
        df = self.spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", kafka_servers) \
            .option("subscribe", topic) \
            .option("startingOffsets", "latest") \
            .load()
        
        print("  Stream Kafka connecte avec succes!\n")
        return df
    
    def process_stream(self, df):
        """Traite le stream de données"""
        schema = self.define_schema()
        
        # Convertir les données JSON
        df_parsed = df.selectExpr("CAST(value AS STRING)") \
            .select(from_json(col("value"), schema).alias("data")) \
            .select("data.*")
        
        # Ajouter une colonne de traitement timestamp
        df_processed = df_parsed.withColumn(
            "processing_time",
            current_timestamp()
        )
        
        # Convertir le timestamp string en timestamp type
        df_processed = df_processed.withColumn(
            "event_timestamp",
            to_timestamp(col("timestamp"))
        )
        
        return df_processed
    
    def detect_anomalies(self, df):
        """Détecte et filtre les anomalies"""
        df_anomalies = df.filter(col("is_anomaly") == True)
        return df_anomalies
    
    def aggregate_by_window(self, df, window_duration="1 minute", slide_duration="30 seconds"):
        """Agrège les données par fenêtre temporelle"""
        df_windowed = df \
            .withWatermark("event_timestamp", "2 minutes") \
            .groupBy(
                window(col("event_timestamp"), window_duration, slide_duration),
                col("sensor_type"),
                col("location")
            ) \
            .agg(
                count("*").alias("count"),
                avg("value").alias("avg_value"),
                min("value").alias("min_value"),
                max("value").alias("max_value"),
                sum(when(col("is_anomaly"), 1).otherwise(0)).alias("anomaly_count")
            )
        
        return df_windowed
    
    def write_to_console(self, df, output_mode="append", trigger_interval="10 seconds"):
        """Écrit le stream vers la console"""
        query = df.writeStream \
            .outputMode(output_mode) \
            .format("console") \
            .trigger(processingTime=trigger_interval) \
            .option("truncate", False) \
            .start()
        
        return query
    
    def write_to_hdfs(self, df, hdfs_path, checkpoint_path, output_mode="append"):
        """Écrit le stream vers HDFS"""
        print(f"\nEcriture vers HDFS: {hdfs_path}")
        
        query = df.writeStream \
            .outputMode(output_mode) \
            .format("parquet") \
            .option("path", hdfs_path) \
            .option("checkpointLocation", checkpoint_path) \
            .trigger(processingTime="30 seconds") \
            .start()
        
        return query
    
    def write_to_csv(self, df, output_path, checkpoint_path):
        """Écrit le stream vers CSV"""
        query = df.writeStream \
            .outputMode("append") \
            .format("csv") \
            .option("path", output_path) \
            .option("checkpointLocation", checkpoint_path) \
            .option("header", True) \
            .trigger(processingTime="30 seconds") \
            .start()
        
        return query

def main():
    """Fonction principale"""
    print("\n" + "=" * 70)
    print("  SPARK STREAMING - TRAITEMENT IOT EN TEMPS REEL")
    print("=" * 70 + "\n")
    
    # Initialiser le processeur
    processor = SparkStreamingProcessor()
    
    # Lire depuis Kafka
    kafka_df = processor.read_from_kafka(
        kafka_servers="kafka:9092",
        topic="iot-sensors"
    )
    
    # Traiter le stream
    processed_df = processor.process_stream(kafka_df)
    
    # === TRAITEMENT 1: Afficher toutes les données ===
    print("\n[STREAM 1] Affichage de toutes les donnees")
    query1 = processor.write_to_console(
        processed_df,
        output_mode="append",
        trigger_interval="10 seconds"
    )
    
    # === TRAITEMENT 2: Détecter et afficher les anomalies ===
    print("\n[STREAM 2] Detection des anomalies")
    anomalies_df = processor.detect_anomalies(processed_df)
    query2 = processor.write_to_console(
        anomalies_df,
        output_mode="append",
        trigger_interval="15 seconds"
    )
    
    # === TRAITEMENT 3: Agrégation par fenêtre temporelle ===
    print("\n[STREAM 3] Agregation par fenetre temporelle")
    windowed_df = processor.aggregate_by_window(
        processed_df,
        window_duration="1 minute",
        slide_duration="30 seconds"
    )
    query3 = processor.write_to_console(
        windowed_df,
        output_mode="update",
        trigger_interval="30 seconds"
    )
    
    # === TRAITEMENT 4: Sauvegarde vers HDFS ===
    print("\n[STREAM 4] Sauvegarde vers HDFS")
    query4 = processor.write_to_hdfs(
        processed_df,
        hdfs_path="hdfs://namenode:9000/iot-data/processed",
        checkpoint_path="hdfs://namenode:9000/iot-data/checkpoint",
        output_mode="append"
    )
    
    # === TRAITEMENT 5: Sauvegarde des anomalies vers HDFS ===
    print("\n[STREAM 5] Sauvegarde des anomalies vers HDFS")
    query5 = processor.write_to_hdfs(
        anomalies_df,
        hdfs_path="hdfs://namenode:9000/iot-data/anomalies",
        checkpoint_path="hdfs://namenode:9000/iot-data/checkpoint-anomalies",
        output_mode="append"
    )
    
    print("\n" + "=" * 70)
    print("  TOUS LES STREAMS SONT ACTIFS")
    print("  Appuyez sur Ctrl+C pour arreter")
    print("=" * 70 + "\n")
    
    # Attendre la fin de tous les streams
    try:
        query1.awaitTermination()
    except KeyboardInterrupt:
        print("\n\nArret des streams...")
        query1.stop()
        query2.stop()
        query3.stop()
        query4.stop()
        query5.stop()
        processor.spark.stop()
        print("Tous les streams ont ete arretes")

if __name__ == "__main__":
    main()