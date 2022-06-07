import json
import uuid

import pytest
from elasticsearch import AsyncElasticsearch

from ..testdata.film_test_data import film_test_doc


@pytest.mark.asyncio
async def test_get_film_detail_positive(es_client: AsyncElasticsearch, redis_client, make_get_request):
    """
    endpoint /films/{film_id} positive test
    """
    # test data inserting
    await es_client.index(
        index="movies",
        id=film_test_doc["id"],
        body=film_test_doc,
    )

    # request making
    response = await make_get_request(f'/films/{film_test_doc["id"]}')
    await es_client.delete(index='movies', id=film_test_doc["id"])

    # result checking
    assert response.status == 200, "wrong status code"

    resp_body = response.body
    resp_body["actors"].sort(key=lambda x: x["id"])
    resp_body["writers"].sort(key=lambda x: x["id"])
    resp_body["genre"].sort()
    assert resp_body == film_test_doc, "wrong elastic resp body"

    # checking that film detail result has been cached
    cache_key = f"film_{film_test_doc['id']}"
    cashed_film_detail = await redis_client.get(cache_key)
    assert cashed_film_detail != None, f"not found {cache_key} in redis"

    film_res_json = json.loads(cashed_film_detail.decode('utf8'))
    for key in ('actors_names', 'writers_names',):
        film_res_json.pop(key, None)
    film_res_json["actors"].sort(key=lambda x: x["id"])
    film_res_json["writers"].sort(key=lambda x: x["id"])
    film_res_json["genre"].sort()
    assert film_res_json == film_test_doc, "wrong redis cache resp body"

    # removing result from cache
    await redis_client.delete(cache_key)


@pytest.mark.asyncio
async def test_get_film_detail_negative(make_get_request):
    """
    endpoint /films/{film_id} negative test
    """
    # request making
    response = await make_get_request(f'/films/{uuid.uuid4()}')

    assert response.status == 404, "wrong status code"

    neg_res_body = {
        "detail": "Film(s) not found"
    }
    assert response.body == neg_res_body, "wrong negative response body"
