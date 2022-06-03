import logging
from functools import lru_cache
from typing import Optional, Union

from aioredis import Redis
from db.redis import get_redis
from fastapi import Depends
from pkg.cache_storage.storage import ABSCacheStorage

logger = logging.getLogger(__name__)

EXPIRATION_TIME_SECONDS = 60 * 5


class RedisCacheService(ABSCacheStorage):
    def __init__(self, redis: Redis) -> None:
        self.redis = redis

    async def get_data(self, key: str) -> Optional[Union[str, bytes]]:
        logger.info(f"getting data from redis by key: {key}")
        try:
            data = await self.redis.get(key)
        except Exception:
            logger.exception("error while getting data from redis")
        else:
            if not data:
                logger.info(f"data not found in redis.")
            return data

    async def set_data(self, key: str, data: Union[str, bytes]):
        logger.info(f"inserting data to redis cache with key: {key}")
        try:
            await self.redis.set(
                key,
                data,
                expire=EXPIRATION_TIME_SECONDS
            )
        except Exception:
            logger.exception("error while inserting data in redis")
        else:
            logger.info(f"successfully added to reddis.")


@lru_cache()
def get_redis_storage_service(
        redis: Redis = Depends(get_redis),
) -> RedisCacheService:
    return RedisCacheService(redis)
