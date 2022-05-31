from functools import lru_cache
from typing import List, Optional, Tuple, Union

from aioredis import Redis
from elasticsearch import AsyncElasticsearch
from fastapi import Depends

from db.elastic import get_elastic
from db.redis import get_redis
from models.genre import Genres
from services.serviceClass import ServiceClass


class GenreService(ServiceClass):

    async def get_list(self,
                       page_size: int,
                       offset_from: int) -> Union[Tuple[Optional[int], Optional[List[Genres]]], Tuple[None, None]]:
        """Получение списка жанров.

        Args:
            page_size: int
            offset_from: int

        Returns: Optional[List[Genres]]
        """
        total, genre_list = await self._get_list_from_cache(f'genre_list{page_size}{offset_from}', Genres)
        if not genre_list:
            total, genre_list = await self._search_match_from_elastic(
                match_query={"match_all": {}},
                index_name='genres',
                page_size=page_size,
                offset_from=offset_from,
                model=Genres
            )

            await self._put_result_list_to_cache(
                cache_id=f'genre_list{page_size}{offset_from}',
                cashed_list=genre_list,
                total=total)
        if not genre_list:
            return None, None

        return total, genre_list

    async def get_by_id(self, genre_id: str) -> Optional[Genres]:
        """Получение жанров по ID

        Args:
            genre_id: str

        Returns: Optional[Genres]

        """
        genre = await self._get_from_cache(genre_id, Genres)
        if not genre:
            genre = await self._get_by_id_from_elastic(
                genre_id,
                'genres',
                Genres)

            if not genre:
                return None
            await self._put_result_to_cache(genre)
        return genre


@lru_cache()
def get_genre_service(
        redis: Redis = Depends(get_redis),
        elastic: AsyncElasticsearch = Depends(get_elastic),
) -> GenreService:
    return GenreService(elastic, redis)
