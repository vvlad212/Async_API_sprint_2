from functools import lru_cache
from typing import Any, List, Optional, Tuple, Union

from aioredis import Redis
from elasticsearch import AsyncElasticsearch
from fastapi import Depends

from db.elastic import get_elastic
from db.redis import get_redis
from models.film import FilmFull
from models.person import Person
from services.serviceClass import ServiceClass


class PersonService(ServiceClass):

    async def get_list(self,
                       page_size: int,
                       offset_from: int) -> Union[Tuple[Optional[int], Optional[List[Person]]], None]:
        """Получение списка персон.

        :param page_size: int
        :param offset_from: int
        :return:List[Person]
        """
        total, person_list = await self._get_list_from_cache(f'person_list{page_size}{offset_from}', Person)
        if not person_list:
            total, person_list = await self._search_match_from_elastic(
                match_query={"match_all": {}},
                index_name='person',
                page_size=page_size,
                offset_from=offset_from,
                model=Person
            )
            await self._put_result_list_to_cache(
                cache_id=f'person_list{page_size}{offset_from}',
                cashed_list=person_list,
                total=total)

        if not person_list:
            return None

        return total, person_list

    async def get_by_name(self,
                          name: str,
                          page_size: int,
                          offset_from: int) -> Union[Tuple[Optional[int], Optional[List[Person]]], Tuple[None, None]]:

        """Поиск персон по имени.

        :param name: str
        :param page_size: int
        :param offset_from: int
        :return: Optional[List[Person], None]
        """
        total, person_list = await self._get_list_from_cache(f'person_name_{name}{page_size}{offset_from}', Person)
        if not person_list:
            total, person_list = await self._search_match_from_elastic(
                match_query={"match": {'full_name': name}},
                index_name='person',
                page_size=page_size,
                offset_from=offset_from,
                model=Person
            )
            await self._put_result_list_to_cache(
                cache_id=f'person_name_{name}{page_size}{offset_from}',
                cashed_list=person_list,
                total=total)
        if not person_list:
            return None, None

        return total, person_list

    async def get_by_id(self, person_id: str) -> Optional[Person]:
        """Получение персоны по ID.

        :param person_id: str
        :return: Optional[Person]
        """
        person = await self._get_from_cache(person_id, Person)
        if not person:

            person = await self._get_by_id_from_elastic(
                selected_id=person_id,
                index_name='person',
                model=Person)
            if not person:
                return None
            await self._put_result_to_cache(person)
        return person


    async def get_film_by_person(
            self,
            person_id: str,
            page_size: int = 10,
            offset_from: int = 0) -> Union[Tuple[Optional[int], Optional[List[FilmFull]]], Tuple[None, None]]:


        """Поиск персон по имени.

        :param person_id: str
        :param page_size: int
        :param offset_from: int
        :return: Optional[List[FilmFull]]
        """
        total, film_list = await self._get_list_from_cache(
            selected_id=f'film_by_person{person_id}{page_size}{offset_from}',
            model=FilmFull)

        if not film_list:
            filters: List[dict] = [
                {"path": "actors", "value": {"actors.id": person_id}},
                {"path": "writers", "value": {"writers.id": person_id}},
            ]
            total, film_list = await self._search_by_nested_values_elastic(
                filter_values=filters,
                index_name='movies',
                condition="should",
                page_size=page_size,
                offset_from=offset_from,
                model=FilmFull
            )
            await self._put_result_list_to_cache(
                cache_id=f'film_by_person{person_id}{page_size}{offset_from}',
                cashed_list=film_list,
                total=total)

        if not film_list:
            return None, None

        return total, film_list


@lru_cache()
def get_person_service(
        redis: Redis = Depends(get_redis),
        elastic: AsyncElasticsearch = Depends(get_elastic),
) -> PersonService:
    return PersonService(elastic, redis)
