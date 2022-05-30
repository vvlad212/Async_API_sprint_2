import logging
import time

from logging import config as logger_conf

from elasticsearch import Elasticsearch

import config
from logger import log_conf

logger_conf.dictConfig(log_conf)
logger = logging.getLogger(__name__)

while True:
    try:
        client = Elasticsearch(hosts=[f'{config.ELASTIC_HOST}:{config.ELASTIC_PORT}'])
        if client.ping():
            logger.info("ES connection OK")
            break
        else:
            logger.info("ES connection FAILED")
            time.sleep(5)
    except Exception as ex:
        logger.error(ex)