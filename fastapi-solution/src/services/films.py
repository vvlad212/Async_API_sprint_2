import json
import logging
from functools import lru_cache
from typing import List, Optional, Tuple

from fastapi import Depends
from models.film import FilmFull
from pkg.cache_storage.redis_storage import get_redis_storage_service
from pkg.cache_storage.storage import ABSCacheStorage
from pkg.storage.elastic_storage import get_elastic_storage_service
from pkg.storage.storage import ABSStorage

from .service import ABSService

logger = logging.getLogger(__name__)

FILMS_INDEX_NAME = "movies"


class FilmService(ABSService):
    def __init__(
        self,
        storage: ABSStorage,
        cache_storage: ABSCacheStorage
    ):
        self.cache_storage = cache_storage
        self.storage = storage

    async def get_by_id(self, film_id: str) -> Optional[FilmFull]:
        """
        Get filmwork by id from es or cache.
        """
        logger.info("FilmService get_by_id started...")

        logger.info("Trying to get film from cache.")
        film_data = await self.cache_storage.get_data(f"film_{film_id}")
        if film_data:
            return FilmFull(**json.loads(film_data))

        logger.info("Trying to get film from elastic.")
        film_doc = await self.storage.get_by_id(film_id, FILMS_INDEX_NAME)
        if film_doc:
            logger.info("Caching film that have been found in storage.")
            await self.cache_storage.set_data(
                f"film_{film_id}",
                json.dumps(film_doc['_source'])
            )
            return FilmFull(**film_doc['_source'])

        return None

    async def get_list(
            self,
            name: Optional[str],
            genres: Optional[List[str]],
            sort: str,
            page_number: int,
            page_size: int
    ) -> Tuple[int, List[FilmFull]]:
        """
        Get filtred filmworks list from cache or es
        """
        logger.info("FilmService get films list started...")

        logger.info(f"Trying to get films from cache.")
        cache_key = "films_{}".format(
            '_'.join(
                filter(
                    None,
                    (
                        name,
                        '_'.join(genres) if genres else None,
                        sort,
                        str(page_number),
                        str(page_size)
                    )
                )
            )
        )
        films_bytes = await self.cache_storage.get_data(cache_key)
        if films_bytes:
            cached_json = json.loads(films_bytes.decode("utf-8"))
            films = [
                FilmFull(**film) for film in cached_json["source"]
            ]
            return cached_json["total_count"], films

        logger.info("Trying to get films from elastic.")
        query_body = await self._create_films_req_body(
            name,
            genres,
            sort,
            page_number,
            page_size
        )
        doc = await self.storage.search(index_name='movies', query=query_body)
        total_count = doc.get('hits', {}).get('total', {}).get('value')
        hits_sources = [
            hit['_source'] for hit in doc.get('hits', {}).get('hits', [])
        ]
        films = [FilmFull(**hit_source) for hit_source in hits_sources]

        if films:
            logger.info("Caching films that have been found in storage.")
            await self.cache_storage.set_data(
                cache_key,
                json.dumps({"total_count": total_count,
                            "source": hits_sources})
            )

        return total_count, films

    async def _create_films_req_body(
            self,
            name: Optional[str],
            genres: Optional[List[str]],
            sort: str,
            page_number: int,
            page_size: int
    ) -> dict:
        body = {
            "query": {
                "bool": {
                    "must": [],
                    "must_not": [],
                    "should": []
                }
            },
            "from": (page_number - 1) * page_size,
            "size": page_size,
            "sort": [],
            "aggs": {}
        }

        if sort == "asc_rating":
            order = "asc"
        else:
            order = "desc"
        body["sort"].append(
            {
                "imdb_rating": {
                    "order": order
                }
            }
        )

        if name:
            body["query"]["bool"]["must"].append(
                {
                    "match": {
                        "title": name
                    }
                }
            )
        else:
            body["query"]["bool"]["must"].append(
                {
                    "match_all": {}
                }
            )

        if genres:
            genres_body = {
                "bool": {
                    "should": []
                }
            }
            for genre in genres:
                genres_body["bool"]["should"].append(
                    {
                        "match": {
                            "genre": genre
                        }
                    },
                )

            body["query"]["bool"]["must"].append(genres_body)
        return body


@lru_cache()
def get_film_service(
        cache_storage: ABSCacheStorage = Depends(get_redis_storage_service),
        elastic: ABSStorage = Depends(get_elastic_storage_service),
) -> FilmService:
    return FilmService(storage=elastic, cache_storage=cache_storage)
