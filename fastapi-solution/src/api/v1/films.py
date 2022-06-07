import math
from enum import Enum
from pydantic import Required
from typing import List, Union

from fastapi import APIRouter, Depends, Query, Path

from api.errors.httperrors import FilmHTTPNotFoundError
from api.models.resp_models import FilmRespModel, FilmsResponseModel
from pkg.pagination.pagination import Paginator
from services.films import FilmService, get_film_service

router = APIRouter()


@router.get(
    '/{film_id}',
    response_model=FilmRespModel,
    tags=["films"],
    responses={
        200: {
            "description": "Film requested by ID",
        },
        404: {
            "description": "Not found",
            "content": {
                "application/json": {
                    "example": {"detail": "Film(s) not found"}
                }
            },
        },
    },
)
async def film_details(
        film_id: str = Path(
            default=Required,
            title="Film id",
            description="UUID of the film to get",
            example="2a090dde-f688-46fe-a9f4-b781a985275e",
        ),
        film_service: FilmService = Depends(get_film_service)
) -> FilmRespModel:
    """
    Get film detail info by film uuid.
    """
    film = await film_service.get_by_id(film_id)
    if not film:
        raise FilmHTTPNotFoundError
    return FilmRespModel.parse_obj(film.dict())


class FilmRating(str, Enum):
    desc = "desc_rating"
    asc = "asc_rating"


@router.get(
    '/',
    response_model=FilmsResponseModel,
    tags=["films"],
    responses={
        200: {
            "description": "Paginated filtered films array.",
        },
    }
)
async def get_films_list(
        name: Union[str, None] = Query(
            default=None,
            title="Name of the film(s)",
            description="Filter by film name.",
            min_length=3,
        ),
        genres: Union[List[str], None] = Query(
            default=None,
            title="Film(s) genres",
            description="Filter by film genre.",
        ),
        sort: FilmRating = Query(
            default=FilmRating.desc,
            title="Film(s) genres",
            description="Sorting order by imdb rating.",
        ),
        paginator: Paginator = Depends(),
        film_service: FilmService = Depends(get_film_service)
) -> FilmsResponseModel:
    """
    Get filtered films list with pagination.
    """
    total_count, films = await film_service.get_list(
        name,
        genres,
        sort,
        paginator.page_number,
        paginator.page_size
    )

    films_res = [FilmRespModel.parse_obj(film.dict()) for film in films]

    return FilmsResponseModel(
        total_count=total_count,
        page_count=math.ceil(total_count / paginator.page_size),
        page_number=paginator.page_number,
        page_size=paginator.page_size,
        records=films_res
    )
