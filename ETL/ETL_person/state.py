import abc
from abc import ABC
from typing import Any

from redis import exceptions as redis_exception
from redis.client import Redis

from backoff import backoff, logger


class BaseStorage(ABC):

    @abc.abstractmethod
    def save_state(self, name: str, key: str, value: str) -> None:
        """Сохранить состояние в постоянное хранилище

        Args:
            state:
            :param value:
            :param key:
            :param name:
        """

    @abc.abstractmethod
    def retrieve_state(self, name: str, key: str) -> dict:
        """Загрузить состояние локально из постоянного хранилища.

        :param name:
        :param key:
        :return:
        """


class RedisStateStorage(BaseStorage):
    def __init__(self, dsl: dict):
        self.dsl = dsl
        self.redis_conn = self.connect_to_redis()
        self.redis_conn.expire('person', 5)

    def retrieve_state(self, name, key) -> str:
        """Загрузить состояние локально из постоянного хранилища."""
        try:
            return self.redis_conn.hget(name, key)

        except redis_exception.ConnectionError as e:
            self.connect_to_redis()

    def save_state(self, name: str, key: str, value: str) -> None:
        """Сохранить состояние в постоянное хранилище"""
        try:
            self.redis_conn.hset(name, key, value)
        except redis_exception.ConnectionError as e:
            logger.error(f'Redis error {e}')
            self.redis_conn = self.connect_to_redis()

    @backoff()
    def connect_to_redis(self):
        """
        Args:
            num: number of reconnection attempts
        """
        logger.info(f'Connecting to redis')
        try:
            return Redis(**self.dsl, charset="utf-8", decode_responses=True)
        except Exception as e:
            logger.error(e)
        logger.info('Successfully connected to redis.')


class State:
    """
    Класс для хранения состояния при работе с данными, чтобы постоянно
    не перечитывать данные с начала.178
    """

    def __init__(self, storage: BaseStorage):
        self.storage = storage

    def set_state(self, name: str, key: str, value: Any) -> None:
        """Установить состояние для определённого ключа"""

        self.storage.save_state(name, key, value)

    def get_state(self, name: str, key: str) -> Any:
        """Получить состояние по определённому ключу"""
        return self.storage.retrieve_state(name, key)
