import logging
import os
from datetime import datetime
from typing import Tuple

import redis

from .backoff_dec import backoff

logger = logging.getLogger(__name__)

DSL = {
    'host': os.environ.get('REDIS_HOST', '127.0.0.1'),
    'port': os.environ.get('REDIS_PORT', '6379')
}


GENRE_STATE_NAME = 'genre'
PERSONS_STATE_NAME = 'person'
FILMWORK_STATE_NAME = 'film_work'


class StateSaver:
    def __init__(self) -> None:
        self.connect_to_redis()

    @backoff(logger)
    def connect_to_redis(self, num: int = 0):
        """
        Args:
            num: number of reconnection attempts
        """
        logger.info(f'Connecting to redis {DSL}')
        try:
            self.r = redis.Redis(
                host=DSL['host'],
                port=DSL['port'],
                charset="utf-8",
                decode_responses=True
            )
        except Exception as e:
            logger.error(e)
            self.connect_to_redis(num=num + 1)
        logger.info('Successfully connected to redis.')

    def get_states(self):
        try:
            current_states = self.r.hgetall("etl_states")
        except (redis.exceptions.ConnectionError, ConnectionRefusedError) as e:
            logger.error(f'Get states error has occurred.\n{e}')
            self.connect_to_redis(1)
            return self.get_states()

        if not current_states:
            logger.info('ETL states not found in redis.')
            current_states = {
                GENRE_STATE_NAME: '',
                PERSONS_STATE_NAME: '',
                FILMWORK_STATE_NAME: '',
            }
            self.r.hset("etl_states", mapping=current_states)
            logger.info('Empty ETL states were saved.')

        return current_states

    def save_states(self, state_names: Tuple[str], new_state_value: datetime):
        try:
            current_states = self.get_states()
            new_datetime_state = new_state_value.strftime(
                "%Y-%m-%d %H:%M:%S.%f %z"
            )
            for state_name in state_names:
                current_states[state_name] = new_datetime_state
            self.r.hset("etl_states", mapping=current_states)
        except (redis.exceptions.ConnectionError, ConnectionRefusedError) as e:
            logger.error(f'Save states error has occurred.\n{e}')
            self.connect_to_redis()
            self.save_states(state_names, new_state_value)
        logger.info(
            f'State(s) for {state_names} has(have) been updated. New state: {new_state_value}'
        )

    def get_working_process_status(self, process_name: str) -> bool:
        try:
            process_status = self.r.get(
                f'process_{process_name}_status'
            )
        except (redis.exceptions.ConnectionError, ConnectionRefusedError) as e:
            logger.error(
                f'Get working process status error has occurred.\n{e}')
            self.connect_to_redis()
            return self.get_working_process_status(process_name)

        if process_status is None:
            self.update_working_process_status(process_name, 0)
            return False

        return bool(int(process_status))

    def update_working_process_status(
        self,
        process_name: str,
        process_status: int
    ):
        try:
            self.r.set(f'process_{process_name}_status', process_status)
        except (redis.exceptions.ConnectionError, ConnectionRefusedError) as e:
            logger.error(
                f'Update working process status error has occurred.\n{e}')
            self.connect_to_redis()
            self.update_working_process_status(process_name, process_status)
        logger.info(
            f'Process {process_name} new status: {bool(process_status)}.')
