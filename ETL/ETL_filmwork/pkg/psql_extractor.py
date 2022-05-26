import logging
import os
from typing import List, Optional, Tuple, Union

import psycopg2
from psycopg2.extras import RealDictCursor

from .backoff_dec import backoff

logger = logging.getLogger(__name__)

RELATED_FW_FETCH_SIZE = 10
DSL = {
    'dbname': os.environ.get('DB_NAME', 'movies_database'),
    'user': os.environ.get('DB_USER', 'app'),
    'password': os.environ.get('DB_PASSWORD', '123qwe'),
    'host': os.environ.get('DB_HOST', '127.0.0.1'),
    'port': os.environ.get('PORT', '5432'),
}

PERSON_MODEL_NAME = 'person'
GENRE_MODEL_NAME = 'genre'
FILM_WORK_MODEL_NAME = 'film_work'


class PsqlExtractor:
    table_mapper = {
        'genre': {
            'table_name': 'content.genre',
            'related_filmwork_table': 'content.genre_film_work',
            'related_column_in_fw_table': 'rfw.genre_id',
        },
        'person': {
            'table_name': 'content.person',
            'related_filmwork_table': 'content.person_film_work',
            'related_column_in_fw_table': 'rfw.person_id',
        },
        'film_work': {'table_name': 'content.film_work', 'related_filmwork_table': ''},
    }

    def __init__(self, updated_model_name: str, last_updation_date: str) -> None:
        self.get_psql_connection()
        self.updated_model_name = updated_model_name
        self.last_updation_date = last_updation_date
        self.updated_table_name = self.table_mapper[updated_model_name]['table_name']
        self.last_modified_time = None
        self.last_modified_filmwork_time = None

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

    def get_updated_filmworks(self) -> list:

        if self.updated_model_name in ('genre', 'person'):
            ids_in_row = tuple(obj['id'] for obj in self.get_updated_rows())
            if not ids_in_row:
                return []
            logger.info(f'Found {len(ids_in_row)} updated rows in {self.updated_model_name} table.')
            for updated_filmwork in self.get_related_filmworks(ids_in_row):
                updated_filmwork_ids = tuple(obj['id'] for obj in updated_filmwork)
                logger.info(f'Found {len(updated_filmwork_ids)} filmworks in need of updating.')
                for fw_batch in self.get_filmworks_info('WHERE fw.id IN %s', (updated_filmwork_ids,)):
                    if fw_batch:
                        yield fw_batch, self.last_modified_time
        elif self.updated_model_name == 'film_work':
            for fw_batch in self.get_filmworks_info('WHERE fw.modified > %s', (self.last_updation_date,)):
                logger.info(f'Found {len(fw_batch)} updated rows in {self.updated_model_name} table.')
                yield fw_batch, self.last_modified_filmwork_time

        return []

    def get_updated_rows(self) -> Optional[list]:
        try:
            self.curs.execute(
                f"""SELECT id, modified
                FROM {self.updated_table_name}
                WHERE modified > %s
                ORDER BY modified
                LIMIT 100;""",
                (self.last_updation_date,),
            )
        except Exception as e:
            logger.error(f'get_updated_rows error has occured.\n{e}')
            self.get_psql_connection(1)
            return self.get_updated_rows()

        res = self.curs.fetchall()
        if res:
            self.last_modified_time = res[-1]['modified']
        return res

    def get_related_filmworks(self, related_ids: Tuple[str]) -> Optional[list]:
        """
        Method returns list of filmwork ids that were affected by
        changes in linked tables.
        """
        try:
            self.curs.execute(
                f"""
                SELECT fw.id, fw.modified
                FROM content.film_work fw
                LEFT JOIN {self.table_mapper[self.updated_model_name]['related_filmwork_table']} rfw ON rfw.film_work_id = fw.id
                WHERE {self.table_mapper[self.updated_model_name]['related_column_in_fw_table']} IN %s
                GROUP BY fw.id
                ORDER BY fw.modified;
                """,
                (related_ids,),
            )
        except Exception as e:
            logger.error(f'get_related_filmworks error has occured.\n{e}')
            self.get_psql_connection(1)
            return self.get_related_filmworks(related_ids)

        while True:
            res = self.curs.fetchmany(RELATED_FW_FETCH_SIZE)
            if not res:
                return
            yield res

    def get_filmworks_info(self, where_cond: str, filter_tuple: Union[Tuple[List[str]], Tuple[str]]) -> Optional[list]:
        """
        Method returs all filmwork info for elasticsearch
        """
        try:
            self.curs_2.execute(
                f"""
                SELECT
                fw.id,
                fw.title,
                fw.description,
                fw.rating,
                fw.type,
                fw.created,
                fw.modified,
                COALESCE (
                    json_agg(
                        DISTINCT jsonb_build_object(
                            'person_role', pfw.role,
                            'person_id', p.id,
                            'person_name', p.full_name
                        )
                    ) FILTER (WHERE p.id is not null),
                    '[]'
                ) as persons,
                array_agg(DISTINCT g.name) as genres
                FROM content.film_work fw
                LEFT JOIN content.person_film_work pfw ON pfw.film_work_id = fw.id
                LEFT JOIN content.person p ON p.id = pfw.person_id
                LEFT JOIN content.genre_film_work gfw ON gfw.film_work_id = fw.id
                LEFT JOIN content.genre g ON g.id = gfw.genre_id
                {where_cond}
                GROUP BY fw.id
                ORDER BY fw.modified ASC;
                """,
                filter_tuple,
            )
        except Exception as e:
            logger.error(f'get_filmworks_info error has occured.\n{e}')
            self.get_psql_connection(1)
            return self.get_filmworks_info(where_cond, filter_tuple)

        while True:
            res = self.curs_2.fetchmany(RELATED_FW_FETCH_SIZE)
            if not res:
                return
            self.last_modified_filmwork_time = res[-1]['modified']
            yield res

    def close_psql_con(self):
        self.pg_conn.close()
