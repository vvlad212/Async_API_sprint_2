import logging
import os

import psycopg2
from psycopg2.extras import RealDictCursor

from .backoff_dec import backoff

logger = logging.getLogger(__name__)

GENRES_FETCH_SIZE = 10
DSL = {
    'dbname': os.environ.get('DB_NAME', 'movies_database'),
    'user': os.environ.get('DB_USER', 'app'),
    'password': os.environ.get('DB_PASSWORD', '123qwe'),
    'host': os.environ.get('DB_HOST', '127.0.0.1'),
    'port': os.environ.get('PORT', '5432'),
}


class PsqlExtractor:
    table_mapper = {
        'genre': {
            'table_name': 'content.genre',
        }
    }

    def __init__(self, last_updation_date: str) -> None:
        self.get_psql_connection()
        self.last_updation_date = last_updation_date
        self.last_modified_time = None

    @backoff(logger)
    def get_psql_connection(self, num: int = 0):
        """
        Args:
            num: number of reconnection attempts
        """
        logger.info(f'Connecting to postgres database {DSL}')
        try:
            with psycopg2.connect(**DSL, cursor_factory=RealDictCursor) as self.pg_conn:
                self.curs = self.pg_conn.cursor()
                self.curs_2 = self.pg_conn.cursor()
        except Exception as e:
            logger.error(e)
            self.get_psql_connection(num=num + 1)
        logger.info('Successfully connected to postgres.')

    def get_updated_genres(self) -> list:
        """
        Get genres rows from content.genres table 
        that have been updated.
        """
        try:
            self.curs.execute(
                f"""
                SELECT g.id, g.name, g.modified
                FROM content.genre g
                WHERE g.modified > %s
                ORDER BY g.modified;
                """,
                (self.last_updation_date,),
            )
        except Exception as e:
            logger.error(f'get_updated_genres error has occured.\n{e}')
            self.get_psql_connection(num=1)
            return self.get_related_filmworks()

        while True:
            res = self.curs.fetchmany(GENRES_FETCH_SIZE)
            if not res:
                return
            yield res, res[-1]['modified']

    def close_psql_con(self):
        self.pg_conn.close()
