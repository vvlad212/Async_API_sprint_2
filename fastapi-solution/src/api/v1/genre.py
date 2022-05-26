from typing import Optional, Union

from fastapi import APIRouter, Depends, Query

from api.errors.httperrors import GenreHTTPNotFoundError
from api.models.resp_models import Genre, ListResponseModel
from services.genre import GenreService, get_genre_service

router = APIRouter()


@router.get('/{genre_id}', response_model=Genre)
async def genre_details(
        genre_id: Union[str, None] = Query(
            default=None,
            title="Genre id",
            min_length=3,
        ),
        genre_service: GenreService = Depends(get_genre_service)) -> Genre:
    """Получение жанра по ID.

    Args:
        genre_id: str
        genre_service: GenreService

    Returns: Genre

    """
    genre = await genre_service.get_by_id(genre_id)
    if not genre:
        raise GenreHTTPNotFoundError
    return Genre(id=genre.id, name=genre.name)


@router.get('/', response_model=ListResponseModel)
async def genre_list(
        page_size: Optional[int] = Query(
            default=10,
            title="Page size",
            alias="page[size]"
        ),
        offset_from: Union[int] = Query(
            default=0,
            title="offset from the first value",
            alias="page[number]"
        ),
        genre_service: GenreService = Depends(get_genre_service)) -> ListResponseModel:
    """Получение списка жанров.

    Args:
        page_size: int
        offset_from: int
        genre_service: GenreService

    Returns: List[Genre]

    """
    total, genre = await genre_service.get_list(page_size, offset_from)
    if not genre:
        raise GenreHTTPNotFoundError

    return ListResponseModel(records=[Genre(id=p.id, name=p.name) for p in genre],
                             total_count=total,
                             current_from=offset_from,
                             page_size=page_size)