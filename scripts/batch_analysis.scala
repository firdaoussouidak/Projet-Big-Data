import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object IoTBatchAnalysis {
  
  def main(args: Array[String]): Unit = {
    
    println("=" * 70)
    println("  ANALYSE BATCH DES DONNEES IOT - SCALA")
    println("=" * 70)
    
    // Créer la session Spark
    val spark = SparkSession.builder()
      .appName("IoT-Batch-Analysis-Scala")
      .config("spark.master", "spark://spark-master:7077")
      .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    
    println(s"\nSpark Version: ${spark.version}")
    println(s"Application ID: ${spark.sparkContext.applicationId}")
    println("-" * 70)
    
    // Définir le schéma
    val schema = StructType(Array(
      StructField("sensor_id", StringType, nullable = true),
      StructField("sensor_type", StringType, nullable = true),
      StructField("location", StringType, nullable = true),
      StructField("value", DoubleType, nullable = true),
      StructField("unit", StringType, nullable = true),
      StructField("timestamp", StringType, nullable = true),
      StructField("is_anomaly", BooleanType, nullable = true),
      StructField("battery_level", DoubleType, nullable = true)
    ))
    
    // Chemins HDFS
    val hdfsPath = "hdfs://namenode:9000/iot-data/processed"
    
    println(s"\n[1] Lecture des donnees depuis HDFS...")
    println(s"    Chemin: $hdfsPath")
    
    // Lire les données depuis HDFS
    val df = spark.read
      .format("parquet")
      .load(hdfsPath)
    
    println(s"    Nombre total d'enregistrements: ${df.count()}")
    
    // Afficher le schéma
    println("\n[2] Schema des donnees:")
    df.printSchema()
    
    // Aperçu des données
    println("\n[3] Apercu des donnees (10 premieres lignes):")
    df.show(10, truncate = false)
    
    // === ANALYSE 1: Statistiques descriptives ===
    println("\n" + "=" * 70)
    println("  ANALYSE 1: STATISTIQUES DESCRIPTIVES")
    println("=" * 70)
    
    val stats = df.groupBy("sensor_type")
      .agg(
        count("*").alias("total_mesures"),
        avg("value").alias("valeur_moyenne"),
        min("value").alias("valeur_min"),
        max("value").alias("valeur_max"),
        stddev("value").alias("ecart_type"),
        sum(when(col("is_anomaly") === true, 1).otherwise(0)).alias("anomalies")
      )
      .orderBy(desc("total_mesures"))
    
    println("\nStatistiques par type de capteur:")
    stats.show(truncate = false)
    
    // === ANALYSE 2: Répartition par localisation ===
    println("\n" + "=" * 70)
    println("  ANALYSE 2: REPARTITION PAR LOCALISATION")
    println("=" * 70)
    
    val locationStats = df.groupBy("location", "sensor_type")
      .agg(
        count("*").alias("nombre_mesures"),
        avg("value").alias("valeur_moyenne"),
        countDistinct("sensor_id").alias("nombre_capteurs")
      )
      .orderBy(desc("nombre_mesures"))
    
    println("\nRepartition des mesures par localisation:")
    locationStats.show(truncate = false)
    
    // === ANALYSE 3: Détection des anomalies ===
    println("\n" + "=" * 70)
    println("  ANALYSE 3: ANALYSE DES ANOMALIES")
    println("=" * 70)
    
    val anomaliesCount = df.filter(col("is_anomaly") === true).count()
    val totalCount = df.count()
    val anomalyRate = (anomaliesCount.toDouble / totalCount.toDouble) * 100
    
    println(s"\nTotal d'anomalies detectees: $anomaliesCount")
    println(f"Taux d'anomalies: $anomalyRate%.2f%%")
    
    val anomaliesByType = df.filter(col("is_anomaly") === true)
      .groupBy("sensor_type", "location")
      .agg(
        count("*").alias("nombre_anomalies"),
        avg("value").alias("valeur_moyenne_anomalie")
      )
      .orderBy(desc("nombre_anomalies"))
    
    println("\nAnomalies par type et localisation:")
    anomaliesByType.show(truncate = false)
    
    // === ANALYSE 4: État des batteries ===
    println("\n" + "=" * 70)
    println("  ANALYSE 4: ETAT DES BATTERIES")
    println("=" * 70)
    
    val batteryStats = df.groupBy("sensor_id")
      .agg(
        avg("battery_level").alias("niveau_batterie_moyen"),
        min("battery_level").alias("niveau_batterie_min"),
        max("battery_level").alias("niveau_batterie_max")
      )
      .withColumn("alerte_batterie", when(col("niveau_batterie_moyen") < 30, "CRITIQUE")
                                      .when(col("niveau_batterie_moyen") < 50, "FAIBLE")
                                      .otherwise("OK"))
      .orderBy("niveau_batterie_moyen")
    
    println("\nEtat des batteries par capteur:")
    batteryStats.show(20, truncate = false)
    
    val criticalBattery = batteryStats.filter(col("alerte_batterie") === "CRITIQUE").count()
    val lowBattery = batteryStats.filter(col("alerte_batterie") === "FAIBLE").count()
    
    println(s"\nCapteurs avec batterie CRITIQUE (<30%%): $criticalBattery")
    println(s"Capteurs avec batterie FAIBLE (30-50%%): $lowBattery")
    
    // === ANALYSE 5: Top capteurs les plus actifs ===
    println("\n" + "=" * 70)
    println("  ANALYSE 5: TOP CAPTEURS LES PLUS ACTIFS")
    println("=" * 70)
    
    val topSensors = df.groupBy("sensor_id", "sensor_type", "location")
      .agg(count("*").alias("nombre_mesures"))
      .orderBy(desc("nombre_mesures"))
      .limit(10)
    
    println("\nTop 10 des capteurs les plus actifs:")
    topSensors.show(truncate = false)
    
    // === SAUVEGARDE DES RÉSULTATS ===
    println("\n" + "=" * 70)
    println("  SAUVEGARDE DES RESULTATS")
    println("=" * 70)
    
    // Sauvegarder les statistiques
    println("\n[1] Sauvegarde des statistiques generales...")
    stats.coalesce(1)
      .write
      .mode("overwrite")
      .option("header", "true")
      .csv("hdfs://namenode:9000/iot-data/analytics/stats")
    println("    Sauvegarde reussie!")
    
    // Sauvegarder les anomalies
    println("\n[2] Sauvegarde des anomalies...")
    df.filter(col("is_anomaly") === true)
      .write
      .mode("overwrite")
      .parquet("hdfs://namenode:9000/iot-data/analytics/anomalies")
    println("    Sauvegarde reussie!")
    
    // Sauvegarder l'état des batteries
    println("\n[3] Sauvegarde de l'etat des batteries...")
    batteryStats.coalesce(1)
      .write
      .mode("overwrite")
      .option("header", "true")
      .csv("hdfs://namenode:9000/iot-data/analytics/battery-status")
    println("    Sauvegarde reussie!")
    
    println("\n" + "=" * 70)
    println("  ANALYSE TERMINEE AVEC SUCCES!")
    println("=" * 70)
    
    // Fermer la session Spark
    spark.stop()
  }
}