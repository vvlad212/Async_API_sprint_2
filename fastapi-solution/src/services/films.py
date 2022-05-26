import json
import logging
from functools import lru_cache
from typing import List, Optional, Tuple

from aioredis import Redis
from elasticsearch import AsyncElasticsearch, NotFoundError
from fastapi import Depends

from db.elastic import get_elastic
from db.redis import get_redis
from models.film import FilmFull

logger = logging.getLogger(__name__)

FILM_CACHE_EXPIRE_IN_SECONDS = 60 * 5  # 5 минут


class FilmService:
    def __init__(self, elastic: AsyncElasticsearch, redis: Redis):
        self.redis = redis
        self.elastic = elastic

    async def get_by_id(self, film_id: str) -> Optional[FilmFull]:
        """
        Get filmwork by id from es or cache
        """
        film = await self._get_film_from_cache(film_id)
        if not film:
            film = await self._get_film_from_elastic(film_id)
            if not film:
                return None

            await self._put_film_to_cache(film)

        return film

    async def _get_film_from_elastic(self, film_id: str) -> Optional[FilmFull]:
        try:
            doc = await self.elastic.get('movies', film_id)
        except NotFoundError:
            return None
        return FilmFull(**doc['_source'])

    async def _get_film_from_cache(self, film_id: str) -> Optional[FilmFull]:
        data = await self.redis.get(film_id)
        if not data:
            return None
        film = FilmFull.parse_raw(data)
        return film

    async def _put_film_to_cache(self, film: FilmFull):
        await self.redis.set(
            film.id,
            film.json(),
            expire=FILM_CACHE_EXPIRE_IN_SECONDS
        )

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
        logger.info("get_films started...")

        cache_key = '_'.join(
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
        logger.info(f"trying to get films from redis cache. Key: {cache_key}")
        total_count, films = await self._get_films_from_cache(cache_key)
        if films:
            return total_count, films

        logger.info("getting films from elastic.")
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
            logger.info("adding films to cache")
            await self.redis.set(
                cache_key,
                json.dumps({"total_count": total_count, "source": hits_sources}),
                expire=FILM_CACHE_EXPIRE_IN_SECONDS
            )

        return total_count, films

    async def _get_films_from_cache(
            self,
            cached_films_key: str
    ) -> Tuple[int, Optional[List[FilmFull]]]:
        films_bytes = await self.redis.get(cached_films_key)
        if films_bytes:
            cached_json = json.loads(films_bytes.decode("utf-8"))
            films = [
                FilmFull(**film) for film in cached_json["source"]
            ]
            return cached_json["total_count"], films
        return 0, None

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
        redis: Redis = Depends(get_redis),
        elastic: AsyncElasticsearch = Depends(get_elastic),
) -> FilmService:
    return FilmService(elastic, redis)
