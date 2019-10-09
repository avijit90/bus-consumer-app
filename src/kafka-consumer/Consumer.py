import logging
import json

from pykafka import KafkaClient
from pykafka.common import OffsetType


class Consumer:

    def __init__(self, host):
        self.host = host
        self.client = None

    def create_kafka_client(self):
        client = KafkaClient(hosts=self.host)
        self.client = client

    def consume_messages(self):

        print(f'connected to kafka')
        print(f'Found following topics :{self.client.topics}')
        topic = self.client.topics['second_topic']
        consumer = topic.get_simple_consumer(auto_offset_reset=OffsetType.EARLIEST)
        for message in consumer:
            if message is not None:
                print('----------------------------')
                print(f'Message offset : {message.offset}')
                print(f'Message value : {json.load(message.value)}')


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
