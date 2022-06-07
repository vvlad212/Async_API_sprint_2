import logging
from functools import lru_cache
from typing import Dict, Union

import backoff
from db import elastic_queries
from db.elastic import get_elastic
from elasticsearch import ConnectionError, Elasticsearch, NotFoundError
from fastapi import Depends
from pkg.storage.storage import ABSStorage

from ..backoff_handlers.req_handler import create_backoff_hdlr

logger = logging.getLogger(__name__)

BACKOFF_MAX_TIME = 60  # sec
back_off_hdlr = create_backoff_hdlr(logger)


class ElasticService(ABSStorage):
    def __init__(self, elastic: Elasticsearch) -> None:
        self.elastic = elastic
        self.queries = elastic_queries

    @backoff.on_exception(
        backoff.fibo,
        ConnectionError,
        max_time=BACKOFF_MAX_TIME,
        on_backoff=back_off_hdlr,
    )
    async def get_by_id(self,
                        id: str,
                        index_name: str) -> Union[Dict, None]:
        """Получения записи из эластика по id.

        :param id:
        :param index_name:
        :return:
        """
        logger.info(f"getting data from elastic index:{index_name} by id:{id}")
        try:
            doc = await self.elastic.get(index_name, id)
        except NotFoundError:
            logger.info(f"data not found.")
            return None
        return doc

    @backoff.on_exception(
        backoff.fibo,
        ConnectionError,
        max_time=BACKOFF_MAX_TIME,
        on_backoff=back_off_hdlr,
    )
    async def search(
            self,
            query: Dict,
            index_name: str) -> Union[Dict, None]:
        """Поиск в эластике c использованием запроса.

        :param query:
        :param index_name:
        :return:
        """
        logger.info(f"searching data in elastic index:{index_name}")
        doc = await self.elastic.search(
            index=index_name,
            body=query
        )
        total_count = doc.get('hits', {}).get('total', {}).get('value')
        if not total_count:
            logger.info(f"searched data not found")       
        return doc


@lru_cache()
def get_elastic_storage_service(
        elastic: Elasticsearch = Depends(get_elastic),
) -> ElasticService:
    return ElasticService(elastic)
