import logging
import re

import certifi
from elasticsearch import Elasticsearch

from ESConnectionException import ESConnectionException


class ElasticConnector:

    def __init__(self):
        self.es_connector = None

    @staticmethod
    def get_elastic_configuration():
        # Parse the auth and host from env:
        bonsai = 'https://ftdcqh6z2x:14zc0p3aji@bus-dashboard-cluste-8058502828.ap-southeast-2.bonsaisearch.net'
        auth = re.search('https\:\/\/(.*)\@', bonsai).group(1).split(':')
        host = bonsai.replace('https://%s:%s@' % (auth[0], auth[1]), '')
        # Connect to cluster over SSL using auth for best security:
        es_config = [{
            'host': host,
            'port': 443,
            'use_ssl': True,
            'http_auth': (auth[0], auth[1]),
            'ca_certs': certifi.where()
        }]
        return es_config

    def test_connection(self):
        if not self.es_connector.ping():
            raise ESConnectionException

        logging.info('Elastic search connection successful !')

    def get_es_connector(self):
        es_config = ElasticConnector.get_elastic_configuration()
        es_connector = Elasticsearch(es_config)
        self.test_connection()
        self.es_connector = es_connector
        return self.es_connector


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
