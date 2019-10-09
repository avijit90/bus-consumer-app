import logging

from Consumer import Consumer
from ElasticConnector import ElasticConnector
from src.Config import es_url, kafka_server_url

es_client = ElasticConnector(es_url)
es_client.create_es_connector()
print(f'connection successful : {es_client.test_connection()}')

consumer = Consumer(kafka_server_url, es_client.es_connector)
consumer.create_kafka_client()
consumer.consume_messages()

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
