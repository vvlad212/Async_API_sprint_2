import asyncio
import logging
import time

import aioredis

from logging import config as logger_conf

import settings
from logger import log_conf

logger_conf.dictConfig(log_conf)
logger = logging.getLogger(__name__)


async def redis_waiters():
    while True:
        try:
            redis_conn = await aioredis.create_redis_pool((settings.REDIS_HOST, settings.REDIS_PORT))
            logger.info("Redis connection OK")
            redis_conn.close()
            await redis_conn.wait_closed()
            return
        except Exception as ex:
            logger.info("Redis connection FAILED")
            logger.error(ex)
            time.sleep(5)

if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(redis_waiters())
    loop.close()
