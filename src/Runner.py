from Consumer import Consumer
import logging

consumer = Consumer('localhost:9092')
consumer.create_kafka_client()
consumer.consume_messages()

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
