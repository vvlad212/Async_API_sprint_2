from typing import Optional, Union

from api.errors.httperrors import (FilmHTTPNotFoundError,
                                   PersonHTTPNotFoundError)
from api.models.resp_models import FilmByPersonModel, ListResponseModel, Person
from fastapi import APIRouter, Depends, Path, Query
from pkg.pagination.pagination import Paginator
from pydantic import Required
from services.person import PersonService, get_person_service

router = APIRouter()


@router.get(
    '/{person_id}',
    response_model=Person,
    tags=["person"],
    responses={
        200: {
            "description": "Person requested by ID",
        },
        404: {
            "description": "Not found",
            "content": {
                "application/json": {
                    "example": {"detail": "Person(s) not found"}
                }
            },
        },
    },
)
async def person_details(
        person_id: Optional[str] = Path(
            title="Person id",
            description="UUID of the person to get.",
            example="f111a93f-0a31-4b6f-bf3b-3a7177915bef",
            default=Required,
        ),
        person_service: PersonService = Depends(get_person_service)) -> Person:
    """Получение персоны по ID.

    Args:
        person_id: str
        person_service: PersonService
    Returns: Person

    """
    person = await person_service.get_by_id(person_id)
    if not person:
        raise PersonHTTPNotFoundError
    return Person(id=person.id, full_name=person.full_name)


@router.get('/',
            response_model=ListResponseModel,
            tags=["person"],
            responses={
                200: {
                    "description": "Person requested list",
                },
                404: {
                    "description": "Not found",
                    "content": {
                        "application/json": {
                            "example": {"detail": "Person(s) not found"}
                        }
                    },
                },
            },
            )
async def person_list(
        paginator: Paginator = Depends(),
        person_service: PersonService = Depends(
            get_person_service)) -> ListResponseModel:
    """Получение списка персон.

    Args:
        paginator: Paginator
        person_service: PersonService

    Returns: List[Person]:

    """

    offset_from = (paginator.page_number-1) * paginator.page_size
    total, list_person = await person_service.get_list(paginator.page_size,
                                                       offset_from)
    if not list_person:
        raise PersonHTTPNotFoundError

    return ListResponseModel(
        records=[Person(id=p.id, full_name=p.full_name) for p in list_person],
        total_count=total,
        current_page=paginator.page_number,
        total_page=int(total / paginator.page_size)+1,
        page_size=paginator.page_size)


@router.get(
    '/search/{person_name}',
    response_model=ListResponseModel,
    tags=["person"],
    responses={
        200: {
            "description": "Person requested by name",
        },
        404: {
            "description": "Not found",
            "content": {
                "application/json": {
                    "example": {"detail": "Person(s) not found"}
                }
            },
        },
    },
)
async def person_by_name(
        name: Optional[str] = Query(
            default=Required,
            title="Person name",
            description="Name of the person to get.",
            example="Tom",
            min_length=2,
        ),
        paginator: Paginator = Depends(),
        person_service: PersonService = Depends(
            get_person_service)) -> ListResponseModel:
    """Поиск персон по имени.

    Args:
        name: str
        paginator: Paginator
        person_service: PersonService

    Returns: List[Person]
    """
    offset_from = (paginator.page_number-1) * paginator.page_size
    total, list_person = await person_service.get_by_name(
        name=name,
        page_size=paginator.page_size,
        offset_from=offset_from
    )

    if not list_person:
        raise PersonHTTPNotFoundError

    return ListResponseModel(
        records=[Person(id=p.id, full_name=p.full_name) for p in list_person],
        total_count=total,
        current_page=paginator.page_number,
        total_page=int(total / paginator.page_size),
        page_size=paginator.page_size)


@router.get(
    '/{person_id}/film',
    response_model=ListResponseModel,
    tags=["person"],
    responses={
        200: {
            "description": "Film requested by person ID",
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
async def film_by_person_id(
        person_id: Optional[str] = Path(
            default=Required,
            title="Person id",
            min_length=3,
            description="UUID of the person to be found in movies.",
            example="e9405a78-8147-4a48-b129-0afa5d7da9dc",
        ),
        paginator: Paginator = Depends(),
        person_service: PersonService = Depends(
            get_person_service)) -> ListResponseModel:
    """Получение фильма по персоне, через ID персоны.

    Args:
        person_id: str
        paginator: Paginator
        person_service: PersonService

    Returns: List[FilmByPersonModel]:

    """
    offset_from = (paginator.page_number-1) * paginator.page_size
    total, film_list = await person_service.get_film_by_person(
        person_id=person_id,
        page_size=paginator.page_size,
        offset_from=offset_from)

    if not film_list:
        raise FilmHTTPNotFoundError

    return ListResponseModel(
        records=[FilmByPersonModel(id=f.id, title=f.title,
                                   imdb_rating=f.imdb_rating) for f in
                 film_list],
        total_count=total,
        current_page=paginator.page_number,
        total_page=int(total / paginator.page_size),
        page_size=paginator.page_size)
