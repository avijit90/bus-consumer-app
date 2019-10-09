import logging

from Consumer import Consumer
from ElasticConnector import ElasticConnector
from src.Config import es_url, kafka_server_url, topic_name


def get_es_connection_status():
    return 'Success' if es_client.test_connection() else 'Failure'


es_client = ElasticConnector(es_url)
es_client.create_es_connector()
print(f'ES connection status : {get_es_connection_status()}')

consumer = Consumer(kafka_server_url, es_client.es_connector, topic_name)
consumer.create_kafka_client()
consumer.consume_messages()

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
