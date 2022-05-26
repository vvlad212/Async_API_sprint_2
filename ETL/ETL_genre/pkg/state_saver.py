import logging
import os
from datetime import datetime, timezone

import redis

from .backoff_dec import backoff

logger = logging.getLogger(__name__)

DSL = {
    'host': os.environ.get('REDIS_HOST', '127.0.0.1'),
    'port': os.environ.get('REDIS_PORT', '6379')
}


ETL_process_status = 'genre_etl_process_status'
ETL_process_state = 'genre_etl_process_state'


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

    def get_state(self):
        try:
            process_state = self.r.get(ETL_process_state)
        except (redis.exceptions.ConnectionError, ConnectionRefusedError) as e:
            logger.error(f'Get state error has occurred.\n{e}')
            self.connect_to_redis(1)
            return self.get_state()

        if process_state is None:
            logger.info(f'{ETL_process_state} not found in redis.')
            min_datetime = datetime.min.replace(tzinfo=timezone.utc)
            self.save_state(min_datetime)
            return min_datetime.strftime(
                "%Y-%m-%d %H:%M:%S.%f %z"
            )

        return process_state

    def save_state(self, new_state_value: datetime):
        try:
            self.r.set(
                ETL_process_state,
                new_state_value.strftime(
                    "%Y-%m-%d %H:%M:%S.%f %z"
                )
            )
        except (redis.exceptions.ConnectionError, ConnectionRefusedError) as e:
            logger.error(f'Save state error has occurred.\n{e}')
            self.connect_to_redis()
            self.save_state(new_state_value)
        logger.info(
            f'{ETL_process_state} has been updated. New state: {new_state_value}'
        )

    def get_working_process_status(self) -> bool:
        try:
            process_status = self.r.get(ETL_process_status)
        except (redis.exceptions.ConnectionError, ConnectionRefusedError) as e:
            logger.error(
                f'Get working process status error has occurred.\n{e}'
            )
            self.connect_to_redis(num=1)
            return self.get_working_process_status()

        if process_status is None:
            logger.info(f'{ETL_process_status} not found in redis.')
            self.update_working_process_status(process_status=0)
            return False

        return bool(int(process_status))

    def update_working_process_status(
        self,
        process_status: int
    ):
        try:
            self.r.set(ETL_process_status, process_status)
        except (redis.exceptions.ConnectionError, ConnectionRefusedError) as e:
            logger.error(
                f'Update working process status error has occurred.\n{e}'
            )
            self.connect_to_redis()
            self.update_working_process_status(process_status)
        logger.info(
            f'{ETL_process_status} changed: {bool(process_status)}.')
