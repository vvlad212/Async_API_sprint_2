from http import HTTPStatus
import json

from elasticsearch import AsyncElasticsearch
import pytest

from ..testdata.films_list_data import test_films_list

MOVIES_INDEX = "movies"
pytestmark = pytest.mark.asyncio


@pytest.fixture(scope='module', autouse=True)
async def bulk_create_tests_data(redis_client, es_client: AsyncElasticsearch):
    create_actions = []
    delete_actions = []
    for film in test_films_list:
        delete_actions.append(
            {
                "delete": {
                    '_index': f'{MOVIES_INDEX}',
                    "_id": film["id"],
                }
            }
        )
        create_actions.extend(
            (
                {
                    "index": {
                        "_index": f"{MOVIES_INDEX}",
                        "_id": f"{film['id']}"
                    }
                },
                film,
            )
        )

    # inserting test data into es
    await es_client.bulk(create_actions, refresh="true")
    yield "bulk_create_tests_data"
    # remove test data from movies index
    await es_client.bulk(delete_actions, refresh="true")
    # remove cached test data from redis
    match = 'films_*'
    cur = b'0'
    while cur:
        cur, keys = await redis_client.scan(cur, match=match)
        for key in keys:
            await redis_client.delete(key)


async def test_get_films_list(redis_client, make_get_request):
    """
    Test GET /films positive test for whole films list with pagination
    """
    # first page
    response = await make_get_request(
        f"/films",
        params={"page[number]": 1, "page[size]": 3}
    )

    assert response.status == HTTPStatus.OK, "wrong status code full films list page 1"

    assert response.body == {
        "total_count": 6,
        "page_count": 2,
        "page_number": 1,
        "page_size": 3,
        "records": test_films_list[:3]
    }

    # second page
    response = await make_get_request(
        f"/films",
        params={"page[number]": 2, "page[size]": 3}
    )

    assert response.status == HTTPStatus.OK, "wrong status code full films list page 2"

    assert response.body == {
        "total_count": 6,
        "page_count": 2,
        "page_number": 2,
        "page_size": 3,
        "records": test_films_list[3:]
    }

    # check that both pages have been cached
    data = await redis_client.get("films_desc_rating_1_3")

    assert json.loads(data) == {"total_count": 6,
                                "source": test_films_list[:3]}, "wrong cache response page 1"

    data = await redis_client.get("films_desc_rating_2_3")
    assert json.loads(data) == {"total_count": 6,
                                "source": test_films_list[3:]}, "wrong cache response page 2"


async def test_get_filtered_films_list(redis_client, make_get_request):
    """
    Test GET /films with filters by film name and genres
    (sort_by asc rating)
    """
    response = await make_get_request(
        f"/films",
        params={
            "name": "movie",
            "genres": ["Action", "Fantasy"],
            "sort": "asc_rating"
        }
    )

    assert response.status == HTTPStatus.OK, "wrong status code filtered films list"
    sorted_res = sorted(
        test_films_list[:2],
        key=lambda x: x["imdb_rating"],
        reverse=False
    )
    assert response.body == {
        "total_count": 2,
        "page_count": 1,
        "page_number": 1,
        "page_size": 20,
        "records": sorted_res
    }, "wrong filtered films list body"

    # check that query result has been cached
    data = await redis_client.get("films_movie_Action_Fantasy_asc_rating_1_20")

    assert json.loads(data) == {
        "total_count": 2,
        "source": sorted_res
    }, "wrong cache response filtered films list body"


async def test_get_empty_films_list(make_get_request):
    """
    Test that GET /films returns empty films list wo error
    """
    response = await make_get_request(
        f"/films",
        params={
            "name": "very long and strange film name",
        }
    )

    assert response.status == HTTPStatus.OK, "wrong status code empty films list"

    assert response.body == {
        'total_count': 0,
        'page_count': 0,
        'page_number': 1,
        'page_size': 20,
        'records': []
    }, "wrong resp body for empty films list"


async def test_get_films_list_negative_page_number(make_get_request):
    """
    Test GET /films with validation error by page_number
    """
    response = await make_get_request(f'/films', params={'page[number]': -1})

    assert response.status == HTTPStatus.UNPROCESSABLE_ENTITY, "wrong validation error page number status"
    assert response.body == {
        "detail": [
            {
                "loc": [
                    "query",
                    "page[number]"
                ],
                "msg": "ensure this value is greater than 0",
                "type": "value_error.number.not_gt",
                "ctx": {
                    "limit_value": 0
                }
            }
        ]
    }, "wrong validation error page number body"


async def test_get_films_list_negative_page_size(make_get_request):
    """
    Test GET /films with validation error by page_size
    """
    response = await make_get_request(f'/films', params={'page[size]': -1})

    assert response.status == HTTPStatus.UNPROCESSABLE_ENTITY, "wrong validation error page size status"

    assert response.body == {
        "detail": [
            {
                "loc": [
                    "query",
                    "page[size]"
                ],
                "msg": "ensure this value is greater than 0",
                "type": "value_error.number.not_gt",
                "ctx": {
                    "limit_value": 0
                }
            }
        ]
    }, "wrong validation error page size body"
