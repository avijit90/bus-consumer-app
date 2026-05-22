import sys
from pathlib import Path

# Keep the existing flat imports working even though the repo uses
# nested source folders that are not valid Python package names.
SRC_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(SRC_DIR / 'kafka-consumer'))
sys.path.insert(0, str(SRC_DIR / 'elastic'))

from BusConsumer import BusConsumer
from ElasticConnector import ElasticConnector
from Config import es_url, topic_name, consumer_config


def get_es_connection_status():
    return 'Success' if es_client.test_connection() else 'Failure'


es_client = ElasticConnector(es_url)
es_client.create_es_connector()
print(f'ES connection status : {get_es_connection_status()}')

consumer = BusConsumer(es_client.es_connector, topic_name, consumer_config)
consumer.create_kafka_client()
consumer.consume_messages()
