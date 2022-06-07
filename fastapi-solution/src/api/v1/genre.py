from typing import Optional

from api.errors.httperrors import GenreHTTPNotFoundError
from api.models.resp_models import Genre, ListResponseModel
from fastapi import APIRouter, Depends, Path, Query
from pkg.pagination.pagination import Paginator
from pydantic import Required
from services.genre import GenreService, get_genre_service

router = APIRouter()



@router.get(
    '/{genre_id}',
    response_model=Genre,
    tags=["genre"],
    responses={
        200: {
            "description": "Genres requested by ID",
        },
        404: {
            "description": "Not found",
            "content": {
                "application/json": {
                    "example": {"detail": "Genres(s) not found"}
                }
            },
        },
    },
)
async def genre_details(
        genre_id: Optional[str] = Path(
            default=Required,
            title="Genre id",
            description="UUID of the genres to get.",
            example="3d8d9bf5-0d90-4353-88ba-4ccc5d2c07ff",
            min_length=8,
        ),
        genre_service: GenreService = Depends(get_genre_service)
) -> Genre:
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


@router.get(
    '/',
    response_model=ListResponseModel,
    tags=["genre"],
    responses={
        200: {
            "description": "Genre requested list",
        },
        404: {
            "description": "Not found",
            "content": {
                "application/json": {
                    "example": {"detail": "Genre(s) not found"}
                }
            },
        },
    },
)
async def genre_list(
        paginator: Paginator = Depends(),
        genre_service: GenreService = Depends(get_genre_service)
) -> ListResponseModel:
    """Получение списка жанров.

    Args:
        paginator: Paginator
        genre_service: GenreService

    Returns: List[Genre]
    """

    offset_from = (paginator.page_number-1) * paginator.page_size
    total, genre = await genre_service.get_list(paginator.page_size,
                                                offset_from)
    if not genre:
        raise GenreHTTPNotFoundError

    return ListResponseModel(
        records=[Genre(id=p.id, name=p.name) for p in genre],
        total_count=total,
        current_page=paginator.page_number,
        total_page=int(total / paginator.page_size)+1,
        page_size=paginator.page_size)
