import json

import pytest

from ..testdata.persondata_in import person_list
from ..testdata.persondata_in import test_films_list, response_film_by_id


@pytest.fixture(scope='session', autouse=True)
async def create_bulk(es_client, redis_client):
    create_bulk = []
    delete_bulk = []
    dataset = {'person': person_list,
               'movies': test_films_list}
    for index in dataset.keys():
        for row in dataset[index]:
            create_bulk.append(
                {
                    "index": {
                        "_index": index,
                        "_id": f"{row['id']}"
                    }
                }
            )
            create_bulk.append(row)

            delete_bulk.append(
                {
                    "delete": {
                        '_index': index,
                        "_id": f"{row['id']}",
                    }
                }
            )

    await es_client.bulk(create_bulk, refresh="true")

    yield
    await redis_client.flushall(True)
    await es_client.bulk(delete_bulk, refresh="true")


@pytest.mark.asyncio
async def test_search_filmbyname(es_client, make_get_request):
    response = await make_get_request(f'/person/8b223e9f-4782-489c-a277-80375aafdced/film',
                                      params={'page[size]': 100})

    result_response_list = {row['id']: row for row in response.body['records']}
    assert response.status == 200
    assert response_film_by_id['total_count'] == response.body['total_count']
    for row in response_film_by_id['records']:
        for keys in row.keys():
            assert row[keys] == result_response_list[str(row['id'])][keys]


@pytest.mark.asyncio
async def test_search_filmbyname_cashed(es_client, redis_client, make_get_request):
    await make_get_request(f'/person/8b223e9f-4782-489c-a277-80375aafdced/film',
                           params={'page[size]': 100})

    response = await redis_client.get(f'film_by_person8b223e9f-4782-489c-a277-80375aafdced1000')
    response = json.loads(response.decode('utf8'))
    result_response_list = {row['_source']['id']: row['_source'] for row in
                            response['data']}
    assert response_film_by_id['total_count'] == response['total']
    for row in response_film_by_id['records']:
        for keys in row.keys():
            assert row[keys] == result_response_list[str(row['id'])][keys]

