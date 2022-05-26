import logging
import os
from typing import List

from elasticsearch import Elasticsearch, helpers

from .backoff_dec import backoff

logger = logging.getLogger(__name__)

DSL = {
    'host': os.environ.get('ES_HOST', '127.0.0.1'),
    'port': os.environ.get('ES_PORT', '9200'),
}

INDEX_NAME = 'movies'


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
        except Exception as e:
            logger.error(e)
            self.connect_to_es(num=num + 1)

    def insert_data_to_es(self, data: list):
        try:
            helpers.bulk(self.es, data)
        except Exception as e:
            logger.error(f'insert_data_to_es Error!\n{e}')
            self.connect_to_es()
            self.insert_data_to_es(data)

    def create_insertable_list(self, filmworks_batch: list) -> List[dict]:
        """
        Method creates list of dict from filmworks_batch
        Args:
            filmworks_batch: batch of filmworks needed for insert or update
             in elasticsearch.
        """
        lst = []
        for fw in filmworks_batch:
            actors = []
            writers = []
            director = ''
            for person in fw['persons']:
                if person['person_role'] == 'director':
                    director = person['person_name']
                    continue
                elif person['person_role'] == 'writer':
                    list_to_add = writers
                elif person['person_role'] == 'actor':
                    list_to_add = actors
                list_to_add.append(
                    {'id': person['person_id'], 'name': person['person_name']}
                )

            new_doc = {
                'id': fw['id'],
                'imdb_rating': fw['rating'],
                'genre': fw['genres'],
                'title': fw['title'],
                'description': fw['description'],
                'director': director,
                'actors_names': ' '.join(act['name'] for act in actors),
                'writers_names': [wr['name'] for wr in writers],
                'actors': actors,
                'writers': writers,
            }
            lst.append(
                {
                    '_index': f'{INDEX_NAME}',
                    '_id': fw['id'],
                    '_type': '_doc',
                    '_source': new_doc,
                }
            )
        return lst

    def load_filmworks_batch(self, filmworks_batch: list):
        """
        Insert batch of filmworks into es
        """
        data_to_insert = self.create_insertable_list(filmworks_batch)
        self.insert_data_to_es(data_to_insert)
