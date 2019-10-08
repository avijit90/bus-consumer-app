import logging
from json import loads
from kafka import KafkaConsumer


class Consumer:

    def __init__(self):
        self.consumer = Consumer.create_kafka_consumer('first_topic')

    @staticmethod
    def create_kafka_consumer(topic_name):
        return KafkaConsumer(
            topic_name,
            bootstrap_servers=['localhost:9092'],
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            group_id='consumer-group',
            value_deserializer=lambda x: loads(x.decode('utf-8')))

    def consume_messages(self):
        for message in self.consumer:
            logging.info(f'Message value : {message.value}')


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
