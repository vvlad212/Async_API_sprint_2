import json
from typing import List

import pytest

from testdata.ES_indexes import mappings
from testdata.genredata_in import genre_list



def create_bulk(data: List[dict], index_name: str):
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
async def test_search_detailed(make_get_request, es_client):
    genre_id = str(genre_list[0]['id'])
    name = genre_list[0]['name']
    bulk_query = create_bulk([genre_list[0]], 'genre')
    await es_client.bulk(bulk_query)

    response = await make_get_request(f'/genre/{genre_id}')
    await es_client.delete(index='genre', id=genre_id)

    assert response.status == 200
    assert len(response.body) == 2
    assert response.body['id'] == genre_id
    assert response.body['name'] == name


@pytest.mark.asyncio
async def test_search_detailed_cashed(make_get_request, es_client, redis_client):
    genre_id = str(genre_list[0]['id'])
    name = genre_list[0]['name']
    bulk_query = create_bulk([genre_list[0]], 'genre')
    await es_client.bulk(bulk_query)

    await make_get_request(f'/genre/{genre_id}')
    await es_client.delete(index='genre', id=genre_id)

    cashed_data = await redis_client.get(genre_id)
    cashed_data = json.loads(cashed_data.decode('utf8'))
    assert cashed_data['id'] == genre_id
    assert cashed_data['name'] == name


@pytest.mark.asyncio
async def test_search_list(es_client, make_get_request):
    bulk_query = create_bulk(genre_list, 'genres')
    await es_client.bulk(bulk_query)
    response = await make_get_request(f'/genre/', params={'page[size]': int(len(genre_list))})
    result_response_list = {row['id']: row for row in response.body['records']}
    assert response.status == 200
    assert len(result_response_list) == len(genre_list)

    for row in genre_list:
        for keys in row.keys():
            assert str(row[keys]) == result_response_list[str(row['id'])][keys]


@pytest.mark.asyncio
async def test_search_list_cached(es_client, redis_client, make_get_request):
    bulk_query = create_bulk(genre_list, 'genres')
    await es_client.bulk(bulk_query)
    await make_get_request(f'/genre/', params={'page[size]': int(len(genre_list))})

    response = await redis_client.get(f'genre_list{int(len(genre_list))}0')
    result_response_list = {json.loads(row)['id']: json.loads(row) for row in
                            json.loads(response.decode('utf8'))['data']}
    assert len(result_response_list) == len(genre_list)
    for row in genre_list:
        for keys in row.keys():
            assert str(row[keys]) == result_response_list[str(row['id'])][keys]
