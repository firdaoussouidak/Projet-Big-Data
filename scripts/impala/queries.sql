-- ============================================
-- REQUETES IMPALA POUR L'ANALYSE DES DONNEES IOT
-- ============================================

-- 1. CREATION DES TABLES EXTERNES SUR HDFS
-- ============================================

-- Table pour les données brutes
CREATE EXTERNAL TABLE IF NOT EXISTS iot_data_raw (
    sensor_id STRING,
    sensor_type STRING,
    location STRING,
    value DOUBLE,
    unit STRING,
    timestamp TIMESTAMP,
    is_anomaly BOOLEAN,
    battery_level DOUBLE,
    processing_time TIMESTAMP,
    event_timestamp TIMESTAMP
)
STORED AS PARQUET
LOCATION 'hdfs://namenode:9000/iot-data/processed';

-- Table pour les anomalies
CREATE EXTERNAL TABLE IF NOT EXISTS iot_anomalies (
    sensor_id STRING,
    sensor_type STRING,
    location STRING,
    value DOUBLE,
    unit STRING,
    timestamp TIMESTAMP,
    is_anomaly BOOLEAN,
    battery_level DOUBLE,
    processing_time TIMESTAMP,
    event_timestamp TIMESTAMP
)
STORED AS PARQUET
LOCATION 'hdfs://namenode:9000/iot-data/anomalies';

-- Rafraîchir les métadonnées
INVALIDATE METADATA iot_data_raw;
INVALIDATE METADATA iot_anomalies;

-- ============================================
-- 2. ANALYSES DESCRIPTIVES
-- ============================================

-- Nombre total d'enregistrements
SELECT COUNT(*) AS total_records
FROM iot_data_raw;

-- Distribution par type de capteur
SELECT 
    sensor_type,
    COUNT(*) AS nombre_mesures,
    ROUND(AVG(value), 2) AS valeur_moyenne,
    ROUND(MIN(value), 2) AS valeur_min,
    ROUND(MAX(value), 2) AS valeur_max,
    SUM(CASE WHEN is_anomaly THEN 1 ELSE 0 END) AS anomalies
FROM iot_data_raw
GROUP BY sensor_type
ORDER BY nombre_mesures DESC;

-- Distribution par localisation
SELECT 
    location,
    COUNT(*) AS nombre_mesures,
    COUNT(DISTINCT sensor_id) AS nombre_capteurs,
    ROUND(AVG(value), 2) AS valeur_moyenne
FROM iot_data_raw
GROUP BY location
ORDER BY nombre_mesures DESC;

-- ============================================
-- 3. ANALYSE DES ANOMALIES
-- ============================================

-- Taux d'anomalies global
SELECT 
    COUNT(*) AS total_mesures,
    SUM(CASE WHEN is_anomaly THEN 1 ELSE 0 END) AS total_anomalies,
    ROUND(100.0 * SUM(CASE WHEN is_anomaly THEN 1 ELSE 0 END) / COUNT(*), 2) AS taux_anomalies_pct
FROM iot_data_raw;

-- Anomalies par type et localisation
SELECT 
    sensor_type,
    location,
    COUNT(*) AS nombre_anomalies,
    ROUND(AVG(value), 2) AS valeur_moyenne,
    ROUND(MIN(value), 2) AS valeur_min,
    ROUND(MAX(value), 2) AS valeur_max
FROM iot_data_raw
WHERE is_anomaly = TRUE
GROUP BY sensor_type, location
ORDER BY nombre_anomalies DESC;

-- Top 10 capteurs avec le plus d'anomalies
SELECT 
    sensor_id,
    sensor_type,
    location,
    COUNT(*) AS nombre_anomalies,
    ROUND(AVG(value), 2) AS valeur_moyenne_anomalie
FROM iot_data_raw
WHERE is_anomaly = TRUE
GROUP BY sensor_id, sensor_type, location
ORDER BY nombre_anomalies DESC
LIMIT 10;

-- ============================================
-- 4. ANALYSE TEMPORELLE
-- ============================================

-- Mesures par heure
SELECT 
    HOUR(event_timestamp) AS heure,
    COUNT(*) AS nombre_mesures,
    SUM(CASE WHEN is_anomaly THEN 1 ELSE 0 END) AS anomalies
FROM iot_data_raw
GROUP BY HOUR(event_timestamp)
ORDER BY heure;

-- Mesures par jour (derniers 7 jours)
SELECT 
    DATE(event_timestamp) AS jour,
    COUNT(*) AS nombre_mesures,
    COUNT(DISTINCT sensor_id) AS capteurs_actifs,
    SUM(CASE WHEN is_anomaly THEN 1 ELSE 0 END) AS anomalies
FROM iot_data_raw
WHERE event_timestamp >= DATE_SUB(NOW(), 7)
GROUP BY DATE(event_timestamp)
ORDER BY jour DESC;

-- ============================================
-- 5. ANALYSE DES BATTERIES
-- ============================================

-- État des batteries par capteur
SELECT 
    sensor_id,
    sensor_type,
    location,
    ROUND(AVG(battery_level), 1) AS niveau_moyen,
    ROUND(MIN(battery_level), 1) AS niveau_min,
    ROUND(MAX(battery_level), 1) AS niveau_max,
    CASE 
        WHEN AVG(battery_level) < 30 THEN 'CRITIQUE'
        WHEN AVG(battery_level) < 50 THEN 'FAIBLE'
        ELSE 'OK'
    END AS statut_batterie
FROM iot_data_raw
GROUP BY sensor_id, sensor_type, location
ORDER BY niveau_moyen ASC;

-- Capteurs nécessitant une intervention
SELECT 
    sensor_id,
    sensor_type,
    location,
    ROUND(MIN(battery_level), 1) AS niveau_batterie_min,
    COUNT(*) AS nombre_mesures
FROM iot_data_raw
GROUP BY sensor_id, sensor_type, location
HAVING MIN(battery_level) < 30
ORDER BY niveau_batterie_min ASC;

-- ============================================
-- 6. ANALYSE PAR TYPE DE CAPTEUR
-- ============================================

-- Température: plages de valeurs
SELECT 
    location,
    COUNT(*) AS nombre_mesures,
    ROUND(AVG(value), 2) AS temp_moyenne,
    ROUND(MIN(value), 2) AS temp_min,
    ROUND(MAX(value), 2) AS temp_max,
    ROUND(STDDEV(value), 2) AS ecart_type
FROM iot_data_raw
WHERE sensor_type = 'temperature'
GROUP BY location
ORDER BY temp_moyenne DESC;

-- Humidité: analyse par localisation
SELECT 
    location,
    COUNT(*) AS nombre_mesures,
    ROUND(AVG(value), 2) AS humidite_moyenne,
    SUM(CASE WHEN value < 40 THEN 1 ELSE 0 END) AS trop_sec,
    SUM(CASE WHEN value > 70 THEN 1 ELSE 0 END) AS trop_humide
FROM iot_data_raw
WHERE sensor_type = 'humidity'
GROUP BY location
ORDER BY humidite_moyenne DESC;

-- ============================================
-- 7. REQUETES AVANCEES
-- ============================================

-- Corrélation entre anomalies et niveau de batterie
SELECT 
    CASE 
        WHEN battery_level < 30 THEN '0-30%'
        WHEN battery_level < 50 THEN '30-50%'
        WHEN battery_level < 70 THEN '50-70%'
        ELSE '70-100%'
    END AS plage_batterie,
    COUNT(*) AS total_mesures,
    SUM(CASE WHEN is_anomaly THEN 1 ELSE 0 END) AS anomalies,
    ROUND(100.0 * SUM(CASE WHEN is_anomaly THEN 1 ELSE 0 END) / COUNT(*), 2) AS taux_anomalies
FROM iot_data_raw
GROUP BY 
    CASE 
        WHEN battery_level < 30 THEN '0-30%'
        WHEN battery_level < 50 THEN '30-50%'
        WHEN battery_level < 70 THEN '50-70%'
        ELSE '70-100%'
    END
ORDER BY plage_batterie;

-- Capteurs les plus performants (moins d'anomalies)
SELECT 
    sensor_id,
    sensor_type,
    location,
    COUNT(*) AS total_mesures,
    SUM(CASE WHEN is_anomaly THEN 1 ELSE 0 END) AS anomalies,
    ROUND(100.0 * SUM(CASE WHEN is_anomaly THEN 1 ELSE 0 END) / COUNT(*), 2) AS taux_anomalies,
    ROUND(AVG(battery_level), 1) AS niveau_batterie_moyen
FROM iot_data_raw
GROUP BY sensor_id, sensor_type, location
HAVING COUNT(*) >= 10
ORDER BY taux_anomalies ASC, total_mesures DESC
LIMIT 10;

-- Rapport de synthèse par localisation
SELECT 
    location,
    COUNT(DISTINCT sensor_id) AS nombre_capteurs,
    COUNT(*) AS total_mesures,
    SUM(CASE WHEN is_anomaly THEN 1 ELSE 0 END) AS total_anomalies,
    ROUND(AVG(battery_level), 1) AS batterie_moyenne,
    COUNT(DISTINCT sensor_type) AS types_capteurs
FROM iot_data_raw
GROUP BY location
ORDER BY total_mesures DESC;

-- ============================================
-- 8. CREATION DE VUES POUR LA VISUALISATION
-- ============================================

-- Vue pour le dashboard en temps réel
CREATE VIEW IF NOT EXISTS v_dashboard_realtime AS
SELECT 
    sensor_type,
    location,
    COUNT(*) AS nombre_mesures,
    ROUND(AVG(value), 2) AS valeur_moyenne,
    SUM(CASE WHEN is_anomaly THEN 1 ELSE 0 END) AS anomalies,
    MAX(event_timestamp) AS derniere_mesure
FROM iot_data_raw
WHERE event_timestamp >= DATE_SUB(NOW(), INTERVAL 1 HOUR)
GROUP BY sensor_type, location;

-- Vue pour l'analyse historique
CREATE VIEW IF NOT EXISTS v_analyse_historique AS
SELECT 
    DATE(event_timestamp) AS date,
    sensor_type,
    COUNT(*) AS nombre_mesures,
    ROUND(AVG(value), 2) AS valeur_moyenne,
    SUM(CASE WHEN is_anomaly THEN 1 ELSE 0 END) AS anomalies
FROM iot_data_raw
GROUP BY DATE(event_timestamp), sensor_type
ORDER BY date DESC, sensor_type;

-- Vue pour les alertes
CREATE VIEW IF NOT EXISTS v_alertes AS
SELECT 
    sensor_id,
    sensor_type,
    location,
    value,
    unit,
    event_timestamp,
    'Anomalie detectee' AS type_alerte
FROM iot_data_raw
WHERE is_anomaly = TRUE
UNION ALL
SELECT 
    sensor_id,
    sensor_type,
    location,
    battery_level AS value,
    '%' AS unit,
    event_timestamp,
    'Batterie faible' AS type_alerte
FROM iot_data_raw
WHERE battery_level < 30
ORDER BY event_timestamp DESC;