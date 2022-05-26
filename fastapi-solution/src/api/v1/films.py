import math
from enum import Enum
from typing import List, Union

from fastapi import APIRouter, Depends, Query

from api.errors.httperrors import FilmHTTPNotFoundError
from api.models.resp_models import FilmRespModel, FilmsResponseModel
from services.films import FilmService, get_film_service

router = APIRouter()


@router.get('/{film_id}', response_model=FilmRespModel)
async def film_details(
        film_id: str,
        film_service: FilmService = Depends(get_film_service)
) -> FilmRespModel:
    film = await film_service.get_by_id(film_id)
    if not film:
        raise FilmHTTPNotFoundError
    return FilmRespModel.parse_obj(film.dict())


class FilmRating(str, Enum):
    desc = "desc_rating"
    asc = "asc_rating"


@router.get('/', response_model=FilmsResponseModel)
async def get_films(
        name: Union[str, None] = Query(
            default=None,
            title="Name of the film(s)",
            min_length=3,
        ),
        genres: Union[List[str], None] = Query(
            alias="genre",
            default=None,
            title="Film(s) genres",
        ),
        sort: FilmRating = "desc_rating",
        page_number: int = Query(gt=0, default=1),
        page_size: int = 20,
        film_service: FilmService = Depends(get_film_service)) -> FilmsResponseModel:

    total_count, films = await film_service.get_films(
        name,
        genres,
        sort,
        page_number,
        page_size
    )

    films_res = [FilmRespModel.parse_obj(film.dict()) for film in films]

    if not films_res:
        raise FilmHTTPNotFoundError

    return FilmsResponseModel(
        total_count=total_count,
        page_count=math.ceil(total_count / page_size),
        page_number=page_number,
        page_size=page_size,
        records=films_res
    )
