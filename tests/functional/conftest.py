import asyncio
import aioredis
import aiohttp
import pytest
from logging import getLogger

from typing import Optional
from dataclasses import dataclass
from multidict import CIMultiDictProxy
from elasticsearch import AsyncElasticsearch
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


@pytest.fixture(scope='session')
async def redis_client():
    rd_client = await aioredis.create_redis_pool((REDIS_HOST, REDIS_PORT))
    yield rd_client
    rd_client.close()
    await rd_client.wait_closed()


@pytest.fixture(scope='session')
async def es_client():
    client = AsyncElasticsearch(hosts=f'{ELASTIC_HOST}:{ELASTIC_PORT}')
    yield client
    await client.close()


@pytest.fixture(scope='session')
async def es_client_source():
    client = AsyncElasticsearch(
        hosts=f'{ELASTIC_HOST_SOURCE}:{ELASTIC_PORT_SOURCE}')
    yield client
    await client.close()


@pytest.fixture(scope='session')
async def session():
    session = aiohttp.ClientSession()
    yield session
    await session.close()


@pytest.fixture(scope='session')
async def index_list(es_client_source):
    yield await es_client_source.indices.get_alias()


@pytest.fixture(scope='session')
async def create_indexes(
    es_client: AsyncElasticsearch,
    es_client_source: AsyncElasticsearch,
    index_list: list
):
    async def inner():
        for ind in index_list:
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
    return inner


@pytest.fixture(scope='session')
async def remove_indexes(
    es_client: AsyncElasticsearch,
    index_list: list
):
    async def inner():
        for ind in index_list:
            await es_client.indices.delete(index=ind, ignore=[400, 404])
    return inner


@pytest.fixture(scope='session', autouse=True)
async def init_db(create_indexes, remove_indexes):
    await create_indexes()
    yield session
    await remove_indexes()


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
