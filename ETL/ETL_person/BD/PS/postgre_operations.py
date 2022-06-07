import logging
from typing import Dict, Optional

import psycopg2
from backoff import backoff
from psycopg2.extensions import connection as _connection
from psycopg2.extras import DictCursor

logger = logging.getLogger(__name__)


class PostgresOperations:
    """Класс, содержащий методы для работы с Postgres."""

    conn: Optional[_connection] = None

    def __init__(self, dsl: Dict[str, str]):
        self.dsl = dsl

    @backoff()
    def get_pg_cursor(self):
        """Получение курсора для Postgres.

        Returns:
            connection.cursor:
        """
        self.conn = self.connection_to_pg()
        return self.conn.cursor()

    @backoff()
    def get_data_cursor(self, query: str):
        """Получение данных из БД Postgres.

        Returns:
            connection.cursor:
        """
        try:
            pg_data_cursor = self.get_pg_cursor()
            pg_data_cursor.execute(query)
            return pg_data_cursor
        except psycopg2.Error:
            logger.exception('PG query execution error')
            self.conn = self.connection_to_pg()

    @backoff()
    def connection_to_pg(self):
        """Подключение к БД Postgres.

        Returns:
            connection:
        """
        with psycopg2.connect(**self.dsl,
                              cursor_factory=DictCursor) as connection:
            return connection
