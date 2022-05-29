import logging

from elasticsearch import Elasticsearch, ElasticsearchException
from logging import config as logger_conf

from utils.logger import log_conf

logger_conf.dictConfig(log_conf)
logger = logging.getLogger(__name__)


def wait_es() -> Elasticsearch:
    """Подключение к Elastic.

    Returns:
        Elasticsearch:
    """

    client = Elasticsearch([{'host': '127.0.0.1', 'port': 9200}])
    if client.ping():
        pass
        logger.info("ES connection OK")
    else:
        pass



