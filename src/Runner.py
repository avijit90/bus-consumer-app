from BusConsumer import BusConsumer
from ElasticConnector import ElasticConnector
from src.Config import es_url, topic_name, consumer_config


def get_es_connection_status():
    return 'Success' if es_client.test_connection() else 'Failure'


es_client = ElasticConnector(es_url)
es_client.create_es_connector()
print(f'ES connection status : {get_es_connection_status()}')

consumer = BusConsumer(es_client.es_connector, topic_name, consumer_config)
consumer.create_kafka_client()
consumer.consume_messages()
