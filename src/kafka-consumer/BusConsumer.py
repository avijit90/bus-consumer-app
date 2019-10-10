import json

from confluent_kafka import Consumer, KafkaException, KafkaError

import TopicNotFoundException


class BusConsumer:

    def __init__(self, es_connector, topic_name, consumer_config):
        self.client = None
        self.es_connector = es_connector
        self.topic_name = topic_name
        self.additional_config = consumer_config

    def create_kafka_client(self):
        config = {
            'session.timeout.ms': 6000,
            'default.topic.config': {'auto.offset.reset': 'earliest'},
        }

        config.update(self.additional_config)
        client = Consumer(**config)
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
        print(f'connected to Kafka server')

        if self.topic_name not in self.client.list_topics().topics:
            print(f"Tried connecting to Topic : {self.topic_name}, but it does not exist !")
            print(f"Available topics : {self.client.list_topics().topics}")
            raise TopicNotFoundException

        self.client.subscribe([self.topic_name])
        print(f'Subscribed to topic : {self.topic_name}')

        try:
            while True:
                msg = self.client.poll(timeout=1.0)
                if msg is None:
                    continue
                if msg.error():
                    # Error or event
                    if msg.error().code() == KafkaError.PARTITION_EOF:
                        # End of partition event
                        print(f'Error -> {msg.topic()} {msg.partition()} reached end of offset {msg.offset()}')
                    elif msg.error():
                        # Error
                        raise KafkaException(msg.error())
                else:
                    # Proper message
                    print('----------------------------')
                    print(
                        f'Message with key : {str(msg.key())} at topic={msg.topic()}, partition={msg.partition()}, offset={msg.offset()} :')
                    es_id = self.put_record_in_ES(json.loads(msg.value()))
                    print(f'Message PUT into elastic with id={es_id}')

        except KeyboardInterrupt:
            print('Aborted by user.')

        # Close down consumer to commit final offsets.
        self.client.close()
