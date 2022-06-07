import http
import json
from http import HTTPStatus

import pytest
from api.errors import httperrors

from ..testdata.genredata_in import genre_list

pytestmark = pytest.mark.asyncio


@pytest.fixture(scope='module', autouse=True)
async def create_bulk(es_client, redis_client):
    create_bulk = []
    delete_bulk = []
    for row in genre_list:
        create_bulk.append(
            {
                "index": {
                    "_index": 'genres',
                    "_id": f"{row['id']}"
                }
            }
        )
        create_bulk.append(row)

        delete_bulk.append(
            {
                "delete": {
                    '_index': 'genres',
                    "_id": f"{row['id']}",
                }
            }
        )

    await es_client.bulk(create_bulk, refresh="true")

    yield
    await redis_client.flushall(True)
    await es_client.bulk(delete_bulk, refresh="true")


async def test_get_genre_list(es_client, make_get_request):
    response = await make_get_request(f'/genre/', params={
        'page[size]': int(len(genre_list))})
    assert response.status == HTTPStatus.OK
    result_response_list = {row['id']: row for row in response.body['records']}
    assert len(result_response_list) == len(genre_list)
    for row in genre_list:
        for keys in row.keys():
            assert str(row[keys]) == result_response_list[str(row['id'])][keys]


async def test_get_genre_list_cached(es_client, redis_client,
                                     make_get_request):
    await make_get_request(f'/genre/',
                           params={'page[size]': int(len(genre_list))})
    response = await redis_client.get(f'genre_list{int(len(genre_list))}0')
    result_response_list = {row['_source']['id']: row['_source'] for row in
                            json.loads(response.decode('utf8'))['data']}
    assert len(result_response_list) == len(genre_list)
    for row in genre_list:
        for keys in row.keys():
            assert str(row[keys]) == result_response_list[str(row['id'])][keys]


async def test_wrong_get_genre_detailed(make_get_request, es_client):
    response = await make_get_request(f'/genre/12345678')
    assert response.status == http.HTTPStatus.NOT_FOUND
    assert response.body['detail'] == httperrors.GenreHTTPNotFoundError.detail


async def test_get_genre_detailed(make_get_request, es_client):
    genre_id = str(genre_list[0]['id'])
    name = genre_list[0]['name']
    response = await make_get_request(f'/genre/{genre_id}')
    assert response.status == HTTPStatus.OK
    assert len(response.body) == 2
    assert response.body['id'] == genre_id
    assert response.body['name'] == name


async def test_get_genre_detailed_cashed(make_get_request, es_client,
                                         redis_client):
    genre_id = str(genre_list[0]['id'])
    name = genre_list[0]['name']
    await make_get_request(f'/genre/{genre_id}')
    cashed_data = await redis_client.get(genre_id)
    cashed_data = json.loads(cashed_data.decode('utf8'))
    assert cashed_data['id'] == genre_id
    assert cashed_data['name'] == name
