import json
import logging
from functools import lru_cache
from typing import Optional, Union, Dict, Tuple

from elasticsearch import Elasticsearch, NotFoundError

from db.elastic import get_elastic
from fastapi import Depends
from pkg.elastic_storage.storage import ABSElasticStorage
from db import elastic_queries

logger = logging.getLogger(__name__)

EXPIRATION_TIME_SECONDS = 60 * 5


class ElasticService(ABSElasticStorage):
    def __init__(self, elastic: Elasticsearch) -> None:
        self.elastic = elastic
        self.queries = elastic_queries

    async def get_by_id(self,
                        id: str,
                        index_name: str) -> Union[Dict, None]:
        """Получения записи из эластика по id.

        :param id:
        :param index_name:
        :return:
        """
        try:
            doc = await self.elastic.get(index_name, id)
        except NotFoundError:
            return None
        return doc

    async def search(
            self,
            query: Dict,
            index_name: str) -> Union[Dict, None]:
        """Поиск в эластике c использованием запроса.

        :param query:
        :param index_name:
        :return:
        """
        try:
            doc = await self.elastic.search(index=index_name,
                                            body=query
                                            )
        except NotFoundError:
            return None
        return doc


@lru_cache()
def get_elastic_storage_service(
        elastic: Elasticsearch = Depends(get_elastic),
) -> ElasticService:
    return ElasticService(elastic)
