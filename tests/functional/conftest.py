import asyncio
from dataclasses import dataclass
from logging import getLogger
from typing import Optional

import aiohttp
import aioredis
import pytest
from elasticsearch import AsyncElasticsearch
from multidict import CIMultiDictProxy

from .settings import *

logger = getLogger(__name__)


@dataclass
class HTTPResponse:
    body: dict
    headers: CIMultiDictProxy[str]
    status: int


@pytest.fixture(scope='session')
def event_loop():
    loop = asyncio.get_event_loop()
    yield loop
    loop.close()


async def create_indexes(
        es_client: AsyncElasticsearch,
        es_client_source: AsyncElasticsearch,
        create_index_list: list):
    """Создание индексов в тестовом elastic перед тестами.

    :param es_client:
    :param es_client_source:
    :param create_index_list:
    """
    for ind in create_index_list:
        source_mapping = await es_client_source.indices.get_mapping(ind)
        source_settings = await es_client_source.indices.get_settings(ind)
        await es_client.indices.create(
            index=ind,
            body={
                "settings": {
                    'refresh_interval': source_settings[ind]['settings']['index']['refresh_interval'],
                    'analysis': source_settings[ind]['settings']['index']['analysis']
                },
                "mappings": source_mapping[ind]['mappings']
            }
        )


async def remove_indexes(
        es_client: AsyncElasticsearch,
        remove_index_list: list):
    """Удаление индексов из тестового elastic.

    :param es_client:
    :param remove_index_list:
    """
    for ind in remove_index_list:
        await es_client.indices.delete(index=ind, ignore=[400, 404])


@pytest.fixture(scope='session')
async def es_client():
    """Подключение к тестовому elastic."""
    client = AsyncElasticsearch(hosts=f'{ELASTIC_HOST}:{ELASTIC_PORT}')
    yield client
    await client.close()


@pytest.fixture(scope='session')
async def es_client_source():
    """Подключение к основному elastic.
    Используется для получения индекса
    """
    client = AsyncElasticsearch(
        hosts=f'{ELASTIC_HOST_SOURCE}:{ELASTIC_PORT_SOURCE}')
    yield client
    await client.close()


@pytest.fixture(scope='session', autouse=True)
async def init_db(es_client, es_client_source):
    index_list = await es_client_source.indices.get_alias()
    await create_indexes(es_client, es_client_source, index_list)
    yield
    await remove_indexes(es_client, index_list)


@pytest.fixture(scope='session')
async def redis_client():
    """Подключение к redis."""
    rd_client = await aioredis.create_redis_pool((REDIS_HOST, REDIS_PORT))
    yield rd_client
    rd_client.close()
    await rd_client.wait_closed()


@pytest.fixture(scope='session')
async def session():
    session = aiohttp.ClientSession()
    yield session
    await session.close()


@pytest.fixture
def make_get_request(session):
    async def inner(method: str, params: Optional[dict] = None) -> HTTPResponse:
        params = params or {}
        url = SERVICE_URL + API + method
        async with session.get(url, params=params) as response:
            logger.info(f"Got resp from {response.url}")
            return HTTPResponse(
                body=await response.json(),
                headers=response.headers,
                status=response.status,
            )

    return inner
