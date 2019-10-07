import logging
import re

import certifi
from elasticsearch import Elasticsearch


def create_es_connector():

    # Parse the auth and host from env:
    bonsai = 'https://ftdcqh6z2x:14zc0p3aji@bus-dashboard-cluste-8058502828.ap-southeast-2.bonsaisearch.net'
    auth = re.search('https\:\/\/(.*)\@', bonsai).group(1).split(':')
    host = bonsai.replace('https://%s:%s@' % (auth[0], auth[1]), '')

    # Connect to cluster over SSL using auth for best security:
    es_header = [{
        'host': host,
        'port': 443,
        'use_ssl': True,
        'http_auth': (auth[0], auth[1]),
        'ca_certs': certifi.where()
    }]

    # Instantiate the new Elasticsearch connection:
    return Elasticsearch(es_header)


def connect_elastic_search():
    es = create_es_connector()
    if es.ping():
        print('Connection to ES successful !')
    else:
        print('Could not connect to ES.')
    return es


connect_elastic_search()

if __name__ == '__main__':
    logging.basicConfig(level=logging.ERROR)
