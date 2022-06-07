import json
from http import HTTPStatus

import pytest
from api.errors import httperrors

from ..testdata.persondata_in import person_list

pytestmark = pytest.mark.asyncio

@pytest.fixture(scope='module', autouse=True)
async def create_bulk(es_client, redis_client):
    create_bulk = []
    delete_bulk = []
    for row in person_list:
        create_bulk.append(
            {
                "index": {
                    "_index": 'person',
                    "_id": f"{row['id']}"
                }
            }
        )
        create_bulk.append(row)

        delete_bulk.append(
            {
                "delete": {
                    '_index': 'person',
                    "_id": f"{row['id']}",
                }
            }
        )

    await es_client.bulk(create_bulk, refresh="true")

    yield
    await redis_client.flushall(True)
    await es_client.bulk(delete_bulk, refresh="true")


async def test_get_person_list(es_client, make_get_request):
    response = await make_get_request(f'/person/', params={
        'page[size]': int(len(person_list))})
    assert response.status == HTTPStatus.OK
    result_response_list = {row['id']: row for row in response.body['records']}
    assert len(result_response_list) == len(person_list)
    for row in person_list:
        for keys in row.keys():
            assert str(row[keys]) == result_response_list[str(row['id'])][keys]


async def test_get_person_list_cached(es_client, redis_client,
                                      make_get_request):
    await make_get_request(f'/person/',
                           params={'page[size]': int(len(person_list))})
    response = await redis_client.get(f'person_list{int(len(person_list))}0')
    result_response_list = {row['_source']['id']: row['_source'] for row in
                            json.loads(response.decode('utf8'))['data']}
    assert len(result_response_list) == len(person_list)
    for row in person_list:
        for keys in row.keys():
            assert str(row[keys]) == result_response_list[str(row['id'])][keys]


async def test_wrong_get_person_detailed(make_get_request, es_client):
    response = await make_get_request(f'/person/123')
    assert response.status == HTTPStatus.NOT_FOUND
    assert response.body['detail'] == httperrors.PersonHTTPNotFoundError.detail


async def test_get_person_detailed(make_get_request, es_client):
    person_id = str(person_list[0]['id'])
    full_name = person_list[0]['full_name']
    response = await make_get_request(f'/person/{person_id}')
    assert response.status == HTTPStatus.OK
    assert len(response.body) == 2
    assert response.body['id'] == person_id
    assert response.body['full_name'] == full_name


async def test_get_person_detailed_cashed(make_get_request, es_client,
                                          redis_client):
    person_id = str(person_list[0]['id'])
    full_name = person_list[0]['full_name']
    await make_get_request(f'/person/{person_id}')
    cashed_data = await redis_client.get(person_id)
    cashed_data = json.loads(cashed_data.decode('utf8'))
    assert cashed_data['id'] == person_id
    assert cashed_data['full_name'] == full_name
