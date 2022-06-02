import json
from typing import List

import pytest
from testdata.persondata_in import person_list, film_by_person


async def create_bulk(data: List[dict], index_name: str):
    bulk = []
    for row in data:
        bulk.append(
            {
                "index": {
                    "_index": f"{index_name}",
                    "_id": f"{row['id']}"
                }
            }
        )
        bulk.append(row)
    return bulk


@pytest.mark.asyncio
async def test_search_list(es_client, make_get_request):
    res = await es_client.bulk(await create_bulk(person_list, 'person'))
    if res['errors']:
        response = await make_get_request(f'/person/', params={'page[size]': int(len(person_list))})
        result_response_list = {row['id']: row for row in response.body['records']}
        assert response.status == 200
        assert len(result_response_list) == len(person_list)

        for row in person_list:
            for keys in row.keys():
                assert str(row[keys]) == result_response_list[str(row['id'])][keys]


@pytest.mark.asyncio
async def test_search_list_cached(es_client, redis_client, make_get_request):
    res = await es_client.bulk(await create_bulk(person_list, 'person'))
    if res['errors']:
        await make_get_request(f'/person/', params={'page[size]': int(len(person_list))})
        response = await redis_client.get(f'person_list{int(len(person_list))}0')
        result_response_list = {json.loads(row)['id']: json.loads(row) for row in
                                json.loads(response.decode('utf8'))['data']}
        assert len(result_response_list) == len(person_list)
        for row in person_list:
            for keys in row.keys():
                assert str(row[keys]) == result_response_list[str(row['id'])][keys]


@pytest.mark.asyncio
async def test_search_detailed(make_get_request, es_client):
    person_id = str(person_list[0]['id'])
    full_name = person_list[0]['full_name']
    res = await es_client.bulk(await create_bulk([person_list[0]], 'person'))
    if res['errors']:
        response = await make_get_request(f'/person/{person_id}')
        assert response.status == 200
        assert len(response.body) == 2
        assert response.body['id'] == person_id
        assert response.body['full_name'] == full_name


@pytest.mark.asyncio
async def test_search_detailed_cashed(make_get_request, es_client, redis_client):
    person_id = str(person_list[0]['id'])
    full_name = person_list[0]['full_name']

    res = await es_client.bulk(await create_bulk([person_list[0]], 'person'))
    if res['errors']:
        await make_get_request(f'/person/{person_id}')
        # await es_client.delete(index='person', id=person_id)

        cashed_data = await redis_client.get(person_id)
        cashed_data = json.loads(cashed_data.decode('utf8'))
        assert cashed_data['id'] == person_id
        assert cashed_data['full_name'] == full_name

# @pytest.mark.asyncio
# async def test_search_filmbyname(es_client, make_get_request):
#     response = await make_get_request(f'/person/b5d2b63a-ed1f-4e46-8320-cf52a32be358/film',
#                                       params={'page[size]': 100})
#
#     result_response_list = {row['id']: row for row in response.body['records']}
#     assert response.status == 200
#     assert len(film_by_person) == len(result_response_list)
#     for row in film_by_person:
#         for keys in row.keys():
#             assert row[keys] == result_response_list[str(row['id'])][keys]
#
#
# @pytest.mark.asyncio
# async def test_search_filmbyname_cashed(es_client, redis_client, make_get_request):
#     await make_get_request(f'/person/b5d2b63a-ed1f-4e46-8320-cf52a32be358/film',
#                            params={'page[size]': 100})
#
#     response = await redis_client.get(f'film_by_personb5d2b63a-ed1f-4e46-8320-cf52a32be3581000')
#     result_response_list = {json.loads(row)['id']: json.loads(row) for row in
#                             json.loads(response.decode('utf8'))['data']}
#     assert len(result_response_list) == len(film_by_person)
#     for row in film_by_person:
#         for keys in row.keys():
#             assert row[keys] == result_response_list[str(row['id'])][keys]
