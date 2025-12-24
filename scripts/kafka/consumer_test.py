from kafka import KafkaConsumer
import json
import sys

class KafkaIoTConsumer:
    def __init__(self, topic, bootstrap_servers='localhost:9093', group_id='iot-consumer-group'):
        """Initialise le consumer Kafka"""
        try:
            self.consumer = KafkaConsumer(
                topic,
                bootstrap_servers=[bootstrap_servers],
                group_id=group_id,
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                auto_offset_reset='earliest',  # Lire depuis le début
                enable_auto_commit=True,
                api_version=(2, 8, 0)
            )
            print(f"Consumer connecte au topic '{topic}'")
            print(f"Bootstrap servers: {bootstrap_servers}")
            print(f"Group ID: {group_id}")
            print("-" * 70)
        except Exception as e:
            print(f"Erreur de connexion: {e}")
            sys.exit(1)
    
    def consume_messages(self, max_messages=None):
        """Consomme les messages du topic"""
        print("\nEcoute des messages... (Ctrl+C pour arreter)\n")
        
        count = 0
        try:
            for message in self.consumer:
                count += 1
                
                print(f"[Message {count}]")
                print(f"  Topic: {message.topic}")
                print(f"  Partition: {message.partition}")
                print(f"  Offset: {message.offset}")
                print(f"  Timestamp: {message.timestamp}")
                
                data = message.value
                print(f"  Contenu:")
                print(f"    - Sensor ID: {data.get('sensor_id')}")
                print(f"    - Type: {data.get('sensor_type')}")
                print(f"    - Valeur: {data.get('value')} {data.get('unit')}")
                print(f"    - Location: {data.get('location')}")
                print(f"    - Timestamp: {data.get('timestamp')}")
                print(f"    - Anomalie: {data.get('is_anomaly')}")
                print(f"    - Batterie: {data.get('battery_level')}%")
                print("-" * 70)
                
                if max_messages and count >= max_messages:
                    print(f"\nNombre maximum de messages atteint: {max_messages}")
                    break
                    
        except KeyboardInterrupt:
            print("\n\nArret demande par l'utilisateur")
        finally:
            self.consumer.close()
            print(f"\nTotal de messages consommes: {count}")
    
    def consume_with_filter(self, filter_func, max_messages=None):
        """Consomme les messages avec un filtre"""
        print("\nEcoute des messages filtres... (Ctrl+C pour arreter)\n")
        
        count = 0
        filtered_count = 0
        
        try:
            for message in self.consumer:
                count += 1
                data = message.value
                
                if filter_func(data):
                    filtered_count += 1
                    print(f"[Message filtre {filtered_count}]")
                    print(f"  Sensor: {data.get('sensor_id')}")
                    print(f"  Type: {data.get('sensor_type')}")
                    print(f"  Valeur: {data.get('value')} {data.get('unit')}")
                    print(f"  Anomalie: {data.get('is_anomaly')}")
                    print("-" * 70)
                
                if max_messages and filtered_count >= max_messages:
                    break
                    
        except KeyboardInterrupt:
            print("\n\nArret demande")
        finally:
            self.consumer.close()
            print(f"\nTotal messages traites: {count}")
            print(f"Messages filtres: {filtered_count}")

def main():
    """Fonction principale"""
    print("\n========================================")
    print("  KAFKA CONSUMER TEST")
    print("========================================\n")
    
    consumer = KafkaIoTConsumer(
        topic='iot-sensors',
        bootstrap_servers='localhost:9093',
        group_id='test-consumer-group'
    )
    
    consumer.consume_messages(max_messages=20)

if __name__ == "__main__":
    main()