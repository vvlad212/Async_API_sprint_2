import logging

import redis
from logging import config as logger_conf

from utils.logger import log_conf

logger_conf.dictConfig(log_conf)
logger = logging.getLogger(__name__)


def wait_redis():
    """Подключение к БД Postgres.

    Returns:
        connection:
    """
    pool = redis.ConnectionPool(host='127.0.0.1', port=6379, db=0)
    redis_conn = redis.Redis(connection_pool=pool)
    if redis_conn.ping():
        logger.info("RD connection OK")
    else:
        pass
