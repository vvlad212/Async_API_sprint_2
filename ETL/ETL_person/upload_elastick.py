import logging
import time
from logging import config as logger_conf

import config as services_config
from backoff import backoff
from BD.ES.elastic_operations import Elastic
from BD.ES.ES_indexes import mappings
from BD.PS.postgre_operations import PostgresOperations
from BD.PS.queries import create_query
from etl_services import PersonEtl
from logger import log_conf
from state import RedisStateStorage, State

logger_conf.dictConfig(log_conf)
logger = logging.getLogger(__name__)


class ETL:
    """Класс реализующий работу ETL пайплайна."""

    def __init__(
            self,
            services: list,
            elastic_connection_dsl: dict,
            pg_dsl: dict,
            rd_dsl: dict,
            query_page_size: int = 100,
            update_freq: int = 10,

    ):
        self.service = None
        self.ps = PostgresOperations(pg_dsl)
        self.es = Elastic(dsl=elastic_connection_dsl, mappings=mappings)
        self.state = State(RedisStateStorage(dsl=rd_dsl))
        self.services = services
        self.current_state = '1990-01-01'
        self.update_freq = update_freq
        self.page_size = query_page_size
        self.es.create_index_person()

    @backoff()
    def run(self):
        """Запуск бесконечного ETL процесса."""
        while True:
            for self.service in self.services:
                get_state = self.state.get_state(self.service.index_name, 'modified')
                if get_state is not None:
                    self.current_state = get_state
                for page in iter(self.extract_from_pg()):
                    bulk, modified = self.transform_data(page)
                    self.load_to_elastic(bulk, modified)
                time.sleep(self.update_freq)

    def extract_from_pg(self):
        """Метод получения данных из Postgres."""

        data_cursor = self.ps.get_data_cursor(
            create_query(self.current_state, self.service.query)
        )
        while True:
            data = data_cursor.fetchmany(self.page_size)
            if not data:
                logger.info("Data in elastic is updated")
                break
            yield data

    def transform_data(self, data):
        """Подготовка данных для вставки в Elastic.

        Args:
            data:
        """
        modified = str(data[-1]['modified'])
        bulk = []
        for row in data:
            model_row = self.service.model(**row)
            bulk.append(
                {
                    "index": {
                        "_index": f"{self.service.index_name}",
                        "_id": f"{model_row.id}"
                    }
                }
            )
            bulk.append(dict(model_row))
        return bulk, modified

    def load_to_elastic(self, bulk: list, modified: str) -> None:
        """Загрузка подготовленных данных в Elastic."

        Args:
            bulk:
            modified:
        """
        es_resp = self.es.upload_to_elastic(bulk)
        if not es_resp['errors']:
            self.state.set_state(self.service.index_name, 'modified', modified)
        else:
            logger.error('Write error in ES', es_resp)


if __name__ == '__main__':

    services_list = [PersonEtl()]
    try:
        etl = ETL(
            query_page_size=services_config.PAGE_SIZE,
            elastic_connection_dsl=services_config.ELASTIC_DSL,
            pg_dsl=services_config.PG_DSL,
            rd_dsl=services_config.REDIS_DSL,
            update_freq=services_config.UPDATE_FREQUENCY,
            services=services_list
        )

        etl.run()

    except Exception as ex:
        logger.exception(ex)

    except KeyboardInterrupt:
        pass
