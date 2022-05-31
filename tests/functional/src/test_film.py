import uuid
import pytest
from elasticsearch import AsyncElasticsearch
from testdata.film_test_data import film_test_doc


@pytest.mark.asyncio
async def test_get_film_detail_positive(es_client: AsyncElasticsearch, make_get_request):
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
    assert resp_body == film_test_doc, "wrong resp body"


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
