import logging
import time
from logging import config as logger_conf

from logger import log_conf
from redis.exceptions import ConnectionError
from settings import REDIS_HOST, REDIS_PORT

from redis import Redis

logger_conf.dictConfig(log_conf)
logger = logging.getLogger(__name__)

SLEEPING_TIME = 5  # sec

def is_redis_started(r: Redis):
    logger.info(f"Checking redis connection {REDIS_HOST} {REDIS_PORT}...")
    attempt_n = 0
    while True:
        attempt_n += 1
        logger.info(f"Attempt number {attempt_n}")
        try:
            r.ping()
        except (ConnectionError, ConnectionRefusedError):
            time.sleep(SLEEPING_TIME)
        else:
            return True


if __name__ == '__main__':
    r = Redis(host=REDIS_HOST, port=REDIS_PORT, socket_connect_timeout=1)

    if is_redis_started(r):
        logger.info("Successfully connected to redis.")
    del r
