
from elasticsearch import Elasticsearch, ElasticsearchException


def create_index_person(index_name: str):
    """Проверка индексов.
    Проверка и в случае отсутствия создание индексов."""

    if not client.indices.exists(index_name):
        res = client.indices.create(
            index=index_name,
            body=mapping[index_name]
        )
        return res
