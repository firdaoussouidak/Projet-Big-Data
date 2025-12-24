import json
import random
import time
from datetime import datetime
from kafka import KafkaProducer
import sys
import os

class IoTDataGenerator:
    def __init__(self, kafka_server='localhost:9093', save_to_file=True, output_dir='../../data'):
        """Initialise le générateur de données IoT"""
        # Configuration Kafka
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=[kafka_server],
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                api_version=(2, 8, 0)
            )
            print(f"✓ Connecte a Kafka: {kafka_server}")
        except Exception as e:
            print(f"✗ Erreur de connexion Kafka: {e}")
            sys.exit(1)
        
        # Configuration sauvegarde fichier
        self.save_to_file = save_to_file
        self.output_dir = output_dir
        self.file_handle = None
        
        if self.save_to_file:
            os.makedirs(self.output_dir, exist_ok=True)
            
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            self.output_file = os.path.join(self.output_dir, f'iot_data_{timestamp}.json')

            self.file_handle = open(self.output_file, 'w', encoding='utf-8')
            print(f"✓ Fichier de sauvegarde: {self.output_file}")
        
        self.sensor_types = ['temperature', 'humidity', 'pressure', 'light']

        self.locations = ['Tangier-Centre', 'Tangier-Port', 'Tangier-Aeroport', 
                         'Tangier-Universite', 'Tangier-Zone-Industrielle']

        self.sensor_ids = [f"SENSOR_{i:03d}" for i in range(1, 21)]
    
    def generate_sensor_data(self):
        """Génère une donnée de capteur aléatoire"""
        sensor_type = random.choice(self.sensor_types)
        
        if sensor_type == 'temperature':
            value = round(random.uniform(15.0, 35.0), 2)
            unit = 'Celsius'
        elif sensor_type == 'humidity':
            value = round(random.uniform(30.0, 90.0), 2)
            unit = '%'
        elif sensor_type == 'pressure':
            value = round(random.uniform(980.0, 1030.0), 2)
            unit = 'hPa'
        else:  
            value = round(random.uniform(0.0, 100000.0), 2)
            unit = 'lux'

        is_anomaly = random.random() < 0.05
        if is_anomaly:
            value *= random.uniform(1.5, 2.5)
        
        data = {
            'sensor_id': random.choice(self.sensor_ids),
            'sensor_type': sensor_type,
            'location': random.choice(self.locations),
            'value': round(value, 2),
            'unit': unit,
            'timestamp': datetime.now().isoformat(),
            'is_anomaly': is_anomaly,
            'battery_level': round(random.uniform(20.0, 100.0), 1)
        }
        
        return data
    
    def save_to_local_file(self, data):
        """Sauvegarde les données dans un fichier local"""
        if self.file_handle:
            try:
                self.file_handle.write(json.dumps(data) + '\n')
                self.file_handle.flush()  # Force l'écriture immédiate
                return True
            except Exception as e:
                print(f"✗ Erreur sauvegarde fichier: {e}")
                return False
        return False
    
    def send_to_kafka(self, topic='iot-sensors', num_messages=None, interval=1):
        """Envoie des données vers Kafka ET sauvegarde localement"""
        print(f"\n{'='*70}")
        print(f"  DEMARRAGE DU GENERATEUR IOT")
        print(f"{'='*70}")
        print(f"\n📊 Configuration:")
        print(f"  - Topic Kafka: {topic}")
        print(f"  - Intervalle: {interval}s")
        print(f"  - Messages: {'Infini (Ctrl+C pour arreter)' if num_messages is None else num_messages}")
        print(f"  - Sauvegarde locale: {'✓ OUI' if self.save_to_file else '✗ NON'}")
        if self.save_to_file:
            print(f"  - Fichier: {self.output_file}")
        print(f"\n{'-'*70}\n")
        
        count = 0
        kafka_success = 0
        file_success = 0
        
        try:
            while num_messages is None or count < num_messages:
                # Générer les données
                data = self.generate_sensor_data()
                
                # Envoi vers Kafka
                try:
                    self.producer.send(topic, value=data)
                    kafka_success += 1
                    kafka_status = "✓"
                except Exception as e:
                    kafka_status = "✗"
                    print(f"    Erreur Kafka: {e}")
                
                # Sauvegarde dans fichier
                file_status = ""
                if self.save_to_file:
                    if self.save_to_local_file(data):
                        file_success += 1
                        file_status = " | Fichier: ✓"
                    else:
                        file_status = " | Fichier: ✗"
                
                # Affichage
                count += 1
                anomaly_indicator = "🔴 ANOMALIE" if data['is_anomaly'] else "✓ Normal"
                
                print(f"[{count:4d}] {data['timestamp'][:19]} | "
                      f"{data['sensor_id']} | "
                      f"{data['sensor_type']:11s}: {data['value']:7.2f} {data['unit']:7s} | "
                      f"{data['location']:25s} | "
                      f"{anomaly_indicator:12s} | "
                      f"Kafka: {kafka_status}{file_status}")
                
                time.sleep(interval)
                
        except KeyboardInterrupt:
            print(f"\n\n{'='*70}")
            print("  ARRET DEMANDE PAR L'UTILISATEUR")
            print(f"{'='*70}")
        finally:
            # Statistiques finales
            print(f"\n📊 STATISTIQUES FINALES:")
            print(f"  - Total messages generes: {count}")
            print(f"  - Envoyes vers Kafka: {kafka_success} ({kafka_success/count*100:.1f}%)")
            if self.save_to_file:
                print(f"  - Sauvegardes dans fichier: {file_success} ({file_success/count*100:.1f}%)")
                print(f"  - Fichier: {self.output_file}")
                print(f"  - Taille: {os.path.getsize(self.output_file) / 1024:.2f} KB")
            
            # Fermer les connexions
            self.producer.flush()
            self.producer.close()
            if self.file_handle:
                self.file_handle.close()
            
            print(f"\n✓ Generateur arrete proprement\n")
    
    def generate_batch_file(self, filename=None, num_records=1000):
        """Génère un fichier batch de données pour tests"""
        if filename is None:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = os.path.join(self.output_dir, f'iot_batch_{timestamp}.json')
        
        print(f"\n{'='*70}")
        print(f"  GENERATION BATCH")
        print(f"{'='*70}")
        print(f"\nGeneration de {num_records} enregistrements...")
        print(f"Fichier: {filename}\n")
        
        # Créer le dossier si nécessaire
        os.makedirs(os.path.dirname(filename), exist_ok=True)
        
        with open(filename, 'w', encoding='utf-8') as f:
            for i in range(num_records):
                data = self.generate_sensor_data()
                f.write(json.dumps(data) + '\n')
                
                if (i + 1) % 100 == 0:
                    print(f"  [{i+1}/{num_records}] enregistrements generes...")
        
        file_size = os.path.getsize(filename) / 1024
        print(f"\n✓ Fichier genere avec succes!")
        print(f"  - Chemin: {filename}")
        print(f"  - Taille: {file_size:.2f} KB")
        print(f"  - Enregistrements: {num_records}\n")

if __name__ == "__main__":
    KAFKA_SERVER = 'localhost:9093'
    TOPIC = 'iot-sensors'
    
    print("\n" + "="*70)
    print("  GENERATEUR DE DONNEES IOT")
    print("  Mode: Streaming en temps reel (Kafka + Sauvegarde Fichier)")
    print("="*70)
    
    print("\nConfiguration:")
    interval_input = input("  Intervalle entre messages en secondes [defaut: 2]: ").strip()
    interval = int(interval_input) if interval_input else 2
    
    generator = IoTDataGenerator(
        kafka_server=KAFKA_SERVER,
        save_to_file=True,
        output_dir='../../data'
    )
    
    
    print("\n✓ Appuyez sur Ctrl+C pour arreter le generateur\n")
    generator.send_to_kafka(
        topic=TOPIC,
        num_messages=None,
        interval=interval
    )