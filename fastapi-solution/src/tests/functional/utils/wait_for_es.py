import logging
import time
from logging import config as logger_conf

from elasticsearch import Elasticsearch
from elasticsearch.exceptions import ConnectionError
from logger import log_conf
from settings import (ELASTIC_HOST, ELASTIC_HOST_SOURCE, ELASTIC_PORT,
                      ELASTIC_PORT_SOURCE)
from urllib3.exceptions import NewConnectionError

logger_conf.dictConfig(log_conf)
logger = logging.getLogger(__name__)

SLEEPING_TIME = 5  # sec


def is_elastics_started(es_host, es_port):
    logger.info(f"Checking elastic connection {es_host} {es_port}...")
    attempt_n = 0
    while True:
        attempt_n += 1
        logger.info(f"Attempt number {attempt_n}")
        try:
            es = Elasticsearch(hosts=[f'{es_host}:{es_port}'])
            if not es.ping():
                raise ConnectionRefusedError("Can't ping es")
        except (ConnectionError, ConnectionRefusedError, NewConnectionError):
            time.sleep(SLEEPING_TIME)
        else:
            return True


if __name__ == '__main__':
    elastics_dsls = (
        (ELASTIC_HOST, ELASTIC_PORT),
        (ELASTIC_HOST_SOURCE, ELASTIC_PORT_SOURCE)
    )

    for es_host, es_port in elastics_dsls:
        if is_elastics_started(es_host, es_port):
            logger.info("Successfully connected to elastics.")

        
