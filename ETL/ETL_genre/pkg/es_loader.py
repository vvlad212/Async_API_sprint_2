import logging
import os
from typing import List

from elasticsearch import Elasticsearch, helpers
from elasticsearch.exceptions import ConnectionError, ConnectionTimeout

from .backoff_dec import backoff
from .es_index import index_body

logger = logging.getLogger(__name__)

DSL = {
    'host': os.environ.get('ES_HOST', '127.0.0.1'),
    'port': os.environ.get('ES_PORT', '9200'),
}

INDEX_NAME = 'genres'


class ESLoader:

    def __init__(self):
        self.connect_to_es()

    @backoff(logger)
    def connect_to_es(self, num: int = 0):
        """
        Args:
            num: number of reconnection attempts
        """
        logger.info(f'Connecting to elasticsearch {DSL}')
        try:
            self.es = Elasticsearch(
                [{'host': DSL['host'], 'port': DSL['port']}])
            if self.es.ping():
                logger.info('Successfully connected to elasticsearch.')
            else:
                logger.error('Could not connect to elasticsearch...')
                raise Exception('es con error.')
        except (ConnectionError, ConnectionTimeout) as e:
            logger.error(e)
            self.connect_to_es(num=num + 1)

    def insert_data_to_es(self, data: list):
        try:
            helpers.bulk(self.es, data)
        except (ConnectionError, ConnectionTimeout) as e:
            logger.error(f'insert_data_to_es connection Error!\n{e}')
            self.connect_to_es()
            self.insert_data_to_es(data)
        except Exception as e:
            logger.error(f'insert_data_to_es other Error!\n{e}')

    def create_insertable_list(self, genres_batch: list) -> List[dict]:
        """
        Method creates list of dict from genres_batch
        Args:
            genres_batch: batch of genres needed for insert or update
             in elasticsearch.
        """
        lst = []
        for genre in genres_batch:
            lst.append(
                {
                    '_index': f'{INDEX_NAME}',
                    '_id': genre['id'],
                    '_type': '_doc',
                    '_source': {
                        'id': genre['id'],
                        'name': genre['name'],

                    },
                }
            )
        return lst

    def load_genres_batch(self, filmworks_batch: list):
        """
        Insert batch of genres into es
        """
        data_to_insert = self.create_insertable_list(filmworks_batch)
        self.insert_data_to_es(data_to_insert)

    def check_index(self):
        try:
            res = self.es.indices.exists(index=INDEX_NAME)
        except (ConnectionError, ConnectionTimeout) as e:
            logger.error(f'check_index connection Error!\n{e}')
            self.connect_to_es()
            self.check_index()
        if not res:
            self.create_index()

    def create_index(self):
        try:
            self.es.indices.create(
                index=INDEX_NAME,
                body=index_body
            )
        except (ConnectionError, ConnectionTimeout) as e:
            logger.error(f'create_index connection Error!\n{e}')
            self.connect_to_es()
            self.create_index()
