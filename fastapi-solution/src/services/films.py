import json
import logging
from functools import lru_cache
from typing import List, Optional, Tuple

from elasticsearch import AsyncElasticsearch, NotFoundError
from fastapi import Depends

from db.elastic import get_elastic
from models.film import FilmFull
from pkg.cache_storage.redis_storage import get_redis_storage_service
from pkg.cache_storage.storage import ABSCacheStorage

logger = logging.getLogger(__name__)


class FilmService:
    def __init__(self, elastic: AsyncElasticsearch, cache_storage: ABSCacheStorage):
        self.cache_storage = cache_storage
        self.elastic = elastic

    async def get_by_id(self, film_id: str) -> Optional[FilmFull]:
        """
        Get filmwork by id from es or cache.
        """
        logger.info("FilmService get_by_id started...")

        logger.info("Trying to get film from cache.")
        film_data = await self.cache_storage.get_data(f"film_{film_id}")
        if film_data:
            return FilmFull.parse_raw(film_data)

        logger.info("Trying to get film from elastic.")
        film = await self._get_film_from_elastic(film_id)
        if film:
            await self.cache_storage.set_data(f"film_{film_id}", film.json())
            return film

        return None

    async def _get_film_from_elastic(self, film_id: str) -> Optional[FilmFull]:
        try:
            doc = await self.elastic.get('movies', film_id)
        except NotFoundError:
            return None
        return FilmFull(**doc['_source'])

    async def get_films(
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
        logger.info("FilmService get_films started...")

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
        doc = await self.elastic.search(index='movies', body=query_body)
        total_count = doc.get('hits').get('total').get('value')
        hits_sources = [hit['_source'] for hit in doc['hits'].get('hits', [])]
        films = [FilmFull(**hit_source) for hit_source in hits_sources]

        if films:
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
        elastic: AsyncElasticsearch = Depends(get_elastic),
) -> FilmService:
    return FilmService(elastic, cache_storage)
