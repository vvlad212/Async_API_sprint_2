import json

import pytest
import redis

from testdata.ES_indexes import mappings


@pytest.mark.asyncio
async def test_search_detailed(es_client, make_get_request):
    # if not es_client.indices.exists('genre'):
    #     es_client.indices.create(
    #         index='genre',
    #         body=mappings['genre']
    #     )
    #
    # json_path = '/Users/vladislavzujkov/YandexPracticum/Async_API_sprint_2/tests/functioanal/testdata/test.json'
    # a= json.load(open(json_path))
    # # Заполнение данных для теста
    # await es_client.bulk([a])

    # Выполнение запроса
    response = await make_get_request('/genre/120a21cf-9097-479e-904a-13dd7198c1dd')
    # data = await redis_client.get('120a21cf-9097-479e-904a-13dd7198c1dd')

    # Проверка результата
    assert response.status == 200
    assert len(response.body) == 2
    assert response.body['id'] == '120a21cf-9097-479e-904a-13dd7198c1dd'
    assert response.body['name'] == 'Adventure'


@pytest.mark.asyncio
async def test_search_list(es_client, redis_client, make_get_request):
    # Заполнение данных для теста
    await es_client.bulk(...)

    # Выполнение запроса
    response = await make_get_request('/genre/120a21cf-9097-479e-904a-13dd7198c1dd',
                                      params={'film_id': 'sdfsdgsdfjhosdf'})

    # Проверка результата
    assert response.status == 200
    assert len(response.body) == 2
    assert response.body['id'] == '120a21cf-9097-479e-904a-13dd7198c1dd'
    assert response.body['name'] == 'Adventure'
