from kafka import KafkaProducer, KafkaAdminClient
from kafka.admin import NewTopic
from kafka.errors import TopicAlreadyExistsError
import json
import time

class KafkaIoTProducer:
    def __init__(self, bootstrap_servers='localhost:9093'):
        """Initialise le producer Kafka"""
        self.bootstrap_servers = bootstrap_servers
        
        self.producer = KafkaProducer(
            bootstrap_servers=[bootstrap_servers],
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: k.encode('utf-8') if k else None,
            acks='all',  # Attendre confirmation de tous les replicas
            retries=3,
            api_version=(2, 8, 0)
        )
        
        self.admin_client = KafkaAdminClient(
            bootstrap_servers=[bootstrap_servers],
            api_version=(2, 8, 0)
        )
        
        print(f"Producer Kafka initialise: {bootstrap_servers}")
    
    def create_topic(self, topic_name, num_partitions=3, replication_factor=1):
        """Crée un topic Kafka"""
        try:
            topic = NewTopic(
                name=topic_name,
                num_partitions=num_partitions,
                replication_factor=replication_factor
            )
            self.admin_client.create_topics([topic])
            print(f"Topic '{topic_name}' cree avec succes")
            print(f"  - Partitions: {num_partitions}")
            print(f"  - Replication factor: {replication_factor}")
        except TopicAlreadyExistsError:
            print(f"Topic '{topic_name}' existe deja")
        except Exception as e:
            print(f"Erreur lors de la creation du topic: {e}")
    
    def send_message(self, topic, message, key=None):
        """Envoie un message vers un topic"""
        try:
            future = self.producer.send(
                topic,
                key=key,
                value=message
            )
            
            record_metadata = future.get(timeout=10)
            
            return {
                'success': True,
                'topic': record_metadata.topic,
                'partition': record_metadata.partition,
                'offset': record_metadata.offset
            }
        except Exception as e:
            return {
                'success': False,
                'error': str(e)
            }
    
    def send_batch(self, topic, messages):
        """Envoie un batch de messages"""
        print(f"\nEnvoi de {len(messages)} messages vers '{topic}'...")
        
        success_count = 0
        error_count = 0
        
        for i, message in enumerate(messages, 1):
            result = self.send_message(topic, message)
            
            if result['success']:
                success_count += 1
                if i % 100 == 0:
                    print(f"  [{i}/{len(messages)}] messages envoyes...")
            else:
                error_count += 1
                print(f"  Erreur message {i}: {result['error']}")
        
        self.producer.flush()
        
        print(f"\nResultats:")
        print(f"  - Succes: {success_count}")
        print(f"  - Erreurs: {error_count}")
        
        return success_count, error_count
    
    def list_topics(self):
        """Liste tous les topics"""
        topics = self.admin_client.list_topics()
        print("\nTopics disponibles:")
        for topic in topics:
            print(f"  - {topic}")
        return topics
    
    def close(self):
        """Ferme le producer"""
        self.producer.close()
        self.admin_client.close()
        print("Producer ferme")

def main():
    """Fonction principale pour tester le producer"""
    producer = KafkaIoTProducer(bootstrap_servers='localhost:9093')
    
    producer.create_topic('iot-sensors', num_partitions=3, replication_factor=1)
    
    producer.list_topics()
    
    test_messages = [
        {
            'sensor_id': 'SENSOR_001',
            'sensor_type': 'temperature',
            'value': 25.5,
            'timestamp': '2024-12-23T10:00:00'
        },
        {
            'sensor_id': 'SENSOR_002',
            'sensor_type': 'humidity',
            'value': 65.0,
            'timestamp': '2024-12-23T10:00:05'
        }
    ]
    
    producer.send_batch('iot-sensors', test_messages)
    
    producer.close()

if __name__ == "__main__":
    main()