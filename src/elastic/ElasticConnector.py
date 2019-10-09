import logging
import re

import certifi
from elasticsearch import Elasticsearch


class ElasticConnector:

    def __init__(self, elastic_url):
        self.elastic_url = elastic_url
        self.es_connector = None,
        self.es_config = None

    def create_es_config(self):
        # Parse the auth and host from env:
        auth = re.search('https\:\/\/(.*)\@', self.elastic_url).group(1).split(':')
        host = self.elastic_url.replace('https://%s:%s@' % (auth[0], auth[1]), '')

        # Connect to cluster over SSL using auth for best security:
        es_config = [{
            'host': host,
            'port': 443,
            'use_ssl': True,
            'http_auth': (auth[0], auth[1]),
            'ca_certs': certifi.where()
        }]
        self.es_config = es_config

    def create_es_connector(self):
        self.create_es_config()
        es_connector = Elasticsearch(self.es_config)
        self.es_connector = es_connector

    def test_connection(self):
        return self.es_connector.ping()


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
