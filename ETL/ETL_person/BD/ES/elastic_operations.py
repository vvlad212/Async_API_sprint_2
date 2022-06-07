import logging

from backoff import backoff
from elasticsearch import Elasticsearch, ElasticsearchException

logger = logging.getLogger(__name__)


class Elastic:
    """Класс, содержащий методы для операций с Elastic."""

    def __init__(self, mappings: dict, dsl: dict):
        self.dsl = dsl
        self.client = self.elastic_connection()
        self.mapping = mappings

    def upload_to_elastic(self, bulk_list: list) -> dict:
        """Выполнение bulk запроса к Elastic.

        Args:
            bulk_list:

        Returns:
            dict
        """
        try:
            return self.client.bulk(body=bulk_list)

        except ElasticsearchException:
            logger.error('Lost connection to Elastic')
            self.client = self.elastic_connection()

    @backoff()
    def elastic_connection(self) -> Elasticsearch:
        """Подключение к Elastic.

        Returns:
            Elasticsearch:
        """
        client = Elasticsearch([{'host': self.dsl['host'], 'port': self.dsl['port']}])
        if client.ping():
            logger.info("ES connection OK")
        else:
            raise ElasticsearchException
        return client

    def create_index_person(self):
        """Проверка индексов.
        Проверка и в случае отсутствия создание индексов."""
        for index_name in self.mapping.keys():
            if not self.client.indices.exists(index_name):
                res = self.client.indices.create(
                    index=index_name,
                    body=self.mapping[index_name]
                )
                return res
