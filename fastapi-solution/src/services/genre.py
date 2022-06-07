import json
from functools import lru_cache
from typing import List, Optional, Tuple, Union

from db.elastic_queries import match_query
from fastapi import Depends
from models.genre import Genres
from pkg.cache_storage.redis_storage import get_redis_storage_service
from pkg.cache_storage.storage import ABSCacheStorage
from pkg.storage.elastic_storage import get_elastic_storage_service
from pkg.storage.storage import ABSStorage


class GenreService:

    def __init__(self, elastic: ABSStorage, cache_storage: ABSCacheStorage):
        self.cache_storage = cache_storage
        self.elastic = elastic

    async def get_list(self,
                       page_size: int,
                       offset_from: int) -> Union[Tuple[Optional[int], Optional[List[Genres]]], Tuple[None, None]]:
        """Получение списка жанров.

        Args:
            page_size: int
            offset_from: int

        Returns: Optional[List[Genres]]
        """
        cashed_data = await self.cache_storage.get_data(key=f'genre_list{page_size}{offset_from}')
        if cashed_data:
            genre_list = [Genres(**d['_source']) for d in json.loads(cashed_data.decode('utf8'))['data']]
            total = json.loads(cashed_data.decode('utf8'))['total']
            return total, genre_list

        query = match_query(match_value={"match_all": {}}, offset_from=offset_from, page_size=page_size)
        elastic_response = await self.elastic.search(index_name='genres', query=query)
        total = elastic_response['hits']['total']['value']
        genre_list = [Genres(**p['_source']) for p in elastic_response['hits']['hits']]

        redis_json = {
            'total': total,
            'data': elastic_response['hits']['hits']
        }
        await self.cache_storage.set_data(key=f'genre_list{page_size}{offset_from}',
                                          data=json.dumps(redis_json))

        if not genre_list:
            return None, None

        return total, genre_list

    async def get_by_id(self, genre_id: str) -> Optional[Genres]:
        """Получение жанров по ID

        Args:
            genre_id: str
        Returns: Optional[Genres]
        """

        cashed_data = await self.cache_storage.get_data(key=genre_id)
        if cashed_data:
            genre = Genres.parse_raw(cashed_data)
            return genre

        doc = await self.elastic.get_by_id(id=genre_id, index_name='genres')

        if not doc:
            return None
        genre = Genres(**doc['_source'])
        await self.cache_storage.set_data(key=f"{genre_id}", data=json.dumps(doc['_source']))

        return genre


@lru_cache()
def get_genre_service(
        redis: ABSCacheStorage = Depends(get_redis_storage_service),
        elastic: ABSStorage = Depends(get_elastic_storage_service),
) -> GenreService:
    return GenreService(elastic, redis)
