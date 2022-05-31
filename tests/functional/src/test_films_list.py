from elasticsearch import AsyncElasticsearch
import pytest


MOVIES_INDEX = "movies"


# list for bulk
# lst.append(
#                 {
#                     '_index': f'{MOVIES_INDEX}',
#                     '_id': fw['id'],
#                     '_type': '_doc',
#                     '_source': new_doc,
#                 }
#             )

# TODO: РАЗБЕРИСЬ КАК ДОБАВИТЬ ФИКСТУРУ К ВЫБОРОЧНЫМ ТЕСТАМ ПО СОЗДАНИЮ 
# СПИСКА ФИЛЬМОВ В ЭЛАСТИКЕ
@pytest.mark.asyncio
async def test_get_films_list(es_client: AsyncElasticsearch, redis_client, make_get_request):
    """
    Test GET /films positive test for whole films list with pagination
    """
    pass
    # create with bulk

    # await es_client.bulk(bulk_query)
    # response = await make_get_request(f'/genre/', params={'page[size]': int(len(genre_list))})

    # send query
    # result_response_list = {row['id']: row for row in response.body['records']}

    # check status and resp (with pagination)
    # assert response.status == 200\

    # check second page

    # check that page 1 and page 2 were cached

    # delete everything from MOVIES_INDEX
    # delete from cache by keys 1 and 2


@pytest.mark.asyncio
async def test_get_filtered_films_list(es_client: AsyncElasticsearch, redis_client, make_get_request):
    """
    Test GET /films with filters by film name and genres
    (sort_by asc rating)
    """
    pass
    # ничего не создаём, сразу пытаемся достать


@pytest.mark.asyncio
async def test_get_empty_films_list(es_client: AsyncElasticsearch, redis_client, make_get_request):
    """
    Test that GET /films returns empty films list wo error
    """
    pass


@pytest.mark.asyncio
async def test_get_films_list_negative_page_number(make_get_request):
    """
    Test GET /films with validation error by page_number
    """
    pass


@pytest.mark.asyncio
async def test_get_films_list_negative_page_size(make_get_request):
    """
    Test GET /films with validation error by page_size
    """
    pass

    # name: Union[str, None] = Query(
    #     default=None,
    #     title="Name of the film(s)",
    #     description="Filter by film name.",
    #     min_length=3,
    # ),
    # genres: Union[List[str], None] = Query(
    #     default=None,
    #     title="Film(s) genres",
    #     description="Filter by film genre.",
    # ),
    # sort: FilmRating = Query(
    #     default=FilmRating.desc,
    #     title="Film(s) genres",
    #     description="Sorting order by imdb rating.",
    # ),
    # page_number: int = Query(
    #     default=1,
    #     gt=0,
    #     description="Pagination page number.",
    # ),
    # page_size: int = Query(
    #     default=20,
    #     gt=0,
    #     description="Pagination size number.",
    # ),
