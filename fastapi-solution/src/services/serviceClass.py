import json
from typing import Any, List, Optional, Tuple, Union

from aioredis import Redis
from elasticsearch import AsyncElasticsearch, NotFoundError

from db import elastic_queries
from models.film import FilmFull
from models.genre import Genres
from models.person import Person


class ServiceClass:
    """
    Вспомогательный класс для классов-сервисов.

        Содержит в себе методы по получению информации из эластика,
        а так же метода для работы с кэшем.
    """

    def __init__(self, elastic: AsyncElasticsearch, redis: Redis):
        self.redis = redis
        self.elastic = elastic
        self.PERSON_CACHE_EXPIRE_IN_SECONDS = 60 * 5  # 5 минут
        self.queries = elastic_queries

    async def _search_match_from_elastic(
            self,
            match_query: dict,
            index_name: str,
            page_size: int,
            offset_from: int,
            model: Any) -> Union[Tuple[Optional[int], Union[List[Person], List[Genres]]], Tuple[None, None]]:
        """Поиск в эластике по полю.

        Args:
            match_query: dict
            index_name: str
            page_size: int
            offset_from: int
            model:

        Returns: Union[List[Person], List[Genres], None]

        """
        try:
            query = self.queries.create_match_query(match_query=match_query,
                                                    page=offset_from,
                                                    size=page_size)
            doc = await self.elastic.search(index=index_name, body=json.dumps(query))

        except NotFoundError:
            return None, None
        return doc['hits']['total']['value'], [model(**p['_source']) for p in doc['hits']['hits']]

    async def _search_by_nested_values_elastic(
            self,
            filter_values: list,
            index_name: str,
            page_size: int,
            offset_from: int,
            condition: str,
            model: Any
    ) -> Union[Tuple[Optional[int], Union[List[Person], List[Genres], List[FilmFull]]], Tuple[None, None]]:
        """Поиск в эластике по вложенным полям.

        Args:
            filter_values: list
            index_name: str
            page_size: int
            offset_from: int
            condition: str
            model:

        Returns:Union[Tuple[Optional[int], Union[List[Person], List[Genres], List[FilmFull]]], None]:

        """
        try:
            query = self.queries.create_nested_query(cond=condition,
                                                     nested_filter=filter_values,
                                                     page=offset_from,
                                                     size=page_size
                                                     )
            doc = await self.elastic.search(index=index_name,
                                            body=json.dumps(query)
                                            )
        except NotFoundError:
            return None, None
        return doc['hits']['total']['value'], [model(**p['_source']) for p in doc['hits']['hits']]

    async def _get_by_id_from_elastic(self,
                                      selected_id: str,
                                      index_name: str,
                                      model: Any) -> Union[Person, Genres, None]:
        """Получения записи из эластика по id.

        Args:
            selected_id: str
            index_name: str
            model:

        Returns: Union[Person, Genres, None]
        """
        try:
            doc = await self.elastic.get(index_name, selected_id)
        except NotFoundError:
            return None
        return model(**doc['_source'])

    async def _get_from_cache(self,
                              selected_id: str,
                              model: Any) -> Union[List[Person], List[Genres], None]:
        """ Получение из кеша

        Args:
            selected_id: str
            model:

        Returns:  Union[List[Person], List[Genres], None]

        """

        data = await self.redis.get(selected_id)
        if not data:
            return None

        result = model.parse_raw(data)
        return result

    async def _put_result_to_cache(self, model):
        """Запись ответа эластика в кэш.

        Args:
            model:
        """
        await self.redis.set(model.id, model.json(),
                             expire=self.PERSON_CACHE_EXPIRE_IN_SECONDS)

    async def _get_list_from_cache(
            self,
            selected_id: str,
            model: Any
    ) -> Union[Tuple[Optional[int], Union[List[Person], List[Genres], List[FilmFull]]], Tuple[None, None]]:
        """Получение списков из кеша.

        Args:
            selected_id: str
            model:

        Returns:  Union[Tuple[Optional[int], Union[List[Person], List[Genres], List[FilmFull]]], Tuple[None, None]]

        """
        data = await self.redis.get(selected_id)
        if not data:
            return None, None
        result = [model.parse_raw(d) for d in json.loads(data.decode('utf8'))['data']]
        return json.loads(data.decode('utf8'))['total'], result

    async def _put_result_list_to_cache(self,
                                        cache_id: str,
                                        cashed_list: list,
                                        total: int):
        """Запись в кэш ответов, представляющих собой списки.

        Args:
            cache_id: str
            cashed_list: list
        """
        redis_json = [elem.json() for elem in cashed_list]
        redis_json = {
            'total': total,
            'data': redis_json
        }
        await self.redis.set(
            cache_id,
            json.dumps(redis_json),
            expire=self.PERSON_CACHE_EXPIRE_IN_SECONDS)
