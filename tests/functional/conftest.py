import asyncio
import aioredis
import aiohttp
import pytest

from typing import Optional
from dataclasses import dataclass
from multidict import CIMultiDictProxy
from elasticsearch import AsyncElasticsearch
from .settings import *

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


@pytest.fixture(scope='module')
async def check_index(es_client):
    """Copying indexes from prod elastic to test elastic

    :param es_client:
    :return:
    """
    client_source = AsyncElasticsearch(hosts=f'{ELASTIC_HOST_SOURCE}:{ELASTIC_PORT_SOURCE}')
    client_target = es_client
    keys = await client_source.indices.get_alias()
    for ind in keys:
        source_mapping = await client_source.indices.get_mapping(ind)
        source_settings = await client_source.indices.get_settings(ind)
        if await client_target.indices.exists(ind):
            await es_client.indices.delete(index=ind)
        await client_target.indices.create(
            index=ind,
            body={
                "settings": {
                    'refresh_interval': source_settings[ind]['settings']['index']['refresh_interval'] if
                    'refresh_interval' in source_settings[ind]['settings']['index'] else "1s",

                    'analysis': source_settings[ind]['settings']['index']['analysis'] if
                    'analysis' in source_settings[ind]['settings']['index'] else {},
                },
                "mappings": source_mapping[ind]['mappings']
            }
        )
    await client_source.close()


@pytest.fixture(scope='session')
async def es_client():
    client = AsyncElasticsearch(hosts=f'{ELASTIC_HOST}:{ELASTIC_PORT}')
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
        url = SERVICE_URL + API + method
        async with session.get(url, params=params) as response:
            return HTTPResponse(
                body=await response.json(),
                headers=response.headers,
                status=response.status,
            )

    return inner
