import asyncio
import aioredis
import aiohttp
import pytest

from typing import Optional
from dataclasses import dataclass
from multidict import CIMultiDictProxy
from elasticsearch import AsyncElasticsearch
from elasticsearch import Elasticsearch
import settings
from testdata.ES_indexes import settings as index_settings


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
def check_index(es_client):
    client_source = Elasticsearch([{'host': settings.ELASTIC_HOST_SOURCE, 'port': settings.ELASTIC_PORT_SOURCE}])
    client_target = Elasticsearch([{'host': settings.ELASTIC_HOST, 'port': settings.ELASTIC_PORT}])
    for ind in client_source.indices.get_alias().keys():
        old_mapping = client_source.indices.get_mapping(ind)
        if not client_target.indices.exists(ind):
            client_target.indices.create(
                index=ind,
                body={
                    "settings": index_settings,
                    "mappings": old_mapping[ind]['mappings']}
            )
    client_target.close()
    client_source.close()
    pass


@pytest.fixture(scope='session')
async def es_client():
    client = AsyncElasticsearch(hosts=f'{settings.ELASTIC_HOST}:{settings.ELASTIC_PORT}')
    yield client
    await client.close()


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
