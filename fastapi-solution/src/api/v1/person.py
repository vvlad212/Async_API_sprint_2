from typing import Optional, Union

from fastapi import APIRouter, Depends, Query

from api.errors.httperrors import (FilmHTTPNotFoundError,
                                   PersonHTTPNotFoundError)
from api.models.resp_models import FilmByPersonModel, ListResponseModel, Person
from services.person import PersonService, get_person_service

router = APIRouter()


@router.get(
    '/{person_id}',
    response_model=Person,
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
        person_id: Union[str, None] = Query(
            title="Person id",
            default=None,
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
        page_size: Optional[int] = Query(
            default=10,
            title="Page size",
            alias="page[size]"
        ),
        page_number: Union[int] = Query(
            default=0,
            title="Page number",
            alias="page[number]"
        ),
        person_service: PersonService = Depends(get_person_service)) -> ListResponseModel:
    """Получение списка персон.

    Args:
        page_size: int
        page_number: int
        person_service: PersonService

    Returns: List[Person]:

    """
    offset_from = page_number * page_size
    total, list_person = await person_service.get_list(page_size, offset_from)
    if not list_person:
        raise PersonHTTPNotFoundError

    return ListResponseModel(records=[Person(id=p.id, full_name=p.full_name) for p in list_person],
                             total_count=total,
                             current_page=page_number,
                             total_page=int(total / page_size),
                             page_size=page_size)


@router.get(
    '/search/{person_name}',
    response_model=ListResponseModel,
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
        name: Union[str, None] = Query(
            default=None,
            title="Person name",
            min_length=3,
        ),
        page_size: Optional[int] = Query(
            default=10,
            title="Page size",
            alias="page[size]"
        ),
        page_number: Union[int] = Query(
            default=0,
            title="Page number",
            alias="page[number]"
        ),
        person_service: PersonService = Depends(get_person_service)) -> ListResponseModel:
    """Поиск персон по имени.

    Args:
        name: str
        page_size: int
        page_number: int
        person_service: PersonService

    Returns: List[Person]
    """
    offset_from = page_number * page_size
    total, list_person = await person_service.get_by_name(
        name=name,
        page_size=page_size,
        offset_from=offset_from
    )

    if not list_person:
        raise PersonHTTPNotFoundError

    return ListResponseModel(
        records=[Person(id=p.id, full_name=p.full_name) for p in list_person],
        total_count=total,
        current_page=page_number,
        total_page=int(total / page_size),
        page_size=page_size)


@router.get(
    '/{person_id}/film',
    response_model=ListResponseModel,
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
        person_id: Union[str, None] = Query(
            default=None,
            title="Person id",
            min_length=3,
        ),
        page_size: Optional[int] = Query(
            default=10,
            title="Page size",
            alias="page[size]"
        ),
        page_number: Union[int] = Query(
            default=0,
            title="Page number",
            alias="page[number]"
        ),
        person_service: PersonService = Depends(get_person_service)) -> ListResponseModel:
    """Получение фильма по персоне, через ID персоны.

    Args:
        person_id: str
        page_size: int
        page_number: int
        person_service: PersonService

    Returns: List[FilmByPersonModel]:

    """
    offset_from = page_number * page_size
    total, film_list = await person_service.get_film_by_person(person_id=person_id,
                                                               page_size=page_size,
                                                               offset_from=offset_from)
    if not film_list:
        raise FilmHTTPNotFoundError

    return ListResponseModel(
        records=[FilmByPersonModel(id=f.id, title=f.title, imdb_rating=f.imdb_rating) for f in film_list],
        total_count=total,
        current_page=page_number,
        total_page=int(total / page_size),
        page_size=page_size)
