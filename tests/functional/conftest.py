import asyncio
import aioredis
import aiohttp
import pytest

from typing import Optional
from dataclasses import dataclass
from multidict import CIMultiDictProxy
from elasticsearch import AsyncElasticsearch
import settings
from testdata.ES_indexes import mappings


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
    rd_client = await aioredis.create_redis_pool((settings.REDIS_HOST, settings.REDIS_PORT))
    yield rd_client
    rd_client.close()
    await rd_client.wait_closed()


@pytest.fixture(scope='session')
def check_index(es_client, es_client_source):
    res = es_client.helpers.reindex(es_client_source, "person", "person")
    pass


@pytest.fixture(scope='session')
async def es_client():
    client = AsyncElasticsearch(hosts=f'{settings.ELASTIC_HOST}:{settings.ELASTIC_PORT}')
    yield client
    await client.close()


@pytest.fixture(scope='session')
async def es_client_source():
    client_source = AsyncElasticsearch(hosts=f'{settings.ELASTIC_HOST_SOURCE}:{settings.ELASTIC_PORT_SOURCE}')
    yield client_source
    await client_source.close()


@pytest.fixture(scope='session')
async def session():
    session = aiohttp.ClientSession()
    yield session
    await session.close()


@pytest.fixture
def make_get_request(session):
    async def inner(method: str, params: Optional[dict] = None) -> HTTPResponse:
        params = params or {}
        url = settings.SERVICE_URL + settings.API + method
        async with session.get(url, params=params) as response:
            return HTTPResponse(
                body=await response.json(),
                headers=response.headers,
                status=response.status,
            )

    return inner
