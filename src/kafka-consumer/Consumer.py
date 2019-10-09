import json
import logging

from pykafka import KafkaClient
from pykafka.common import OffsetType


class Consumer:

    def __init__(self, host, es_connector):
        self.host = host
        self.es_connector = es_connector
        self.client = None

    def create_kafka_client(self):
        client = KafkaClient(hosts=self.host)
        self.client = client

    def put_record_in_ES(self, record):
        weed_out_record = False
        for value in record.values():
            if value is "":
                weed_out_record = True
                break

        if not weed_out_record:
            res = self.es_connector.index(index='supreme', body=record)
            return res['_id']
        else:
            return 'Skipped record'

    def consume_messages(self):
        print(f'connected to kafka')
        print(f'Found following topics :{self.client.topics}')
        topic = self.client.topics['bus_supreme']
        consumer = topic.get_simple_consumer(auto_offset_reset=OffsetType.EARLIEST)

        for message in consumer:
            if message is not None:
                print('----------------------------')
                print(f"Read message with key={message.partition_key.decode('utf-8')}")
                es_id = self.put_record_in_ES(json.loads(message.value))
                print(f'Message PUT into elastic with id={es_id}')


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
