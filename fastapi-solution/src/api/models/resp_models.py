from typing import List, Optional, Union

from pydantic import BaseModel, Field


class Person(BaseModel):
    id: str
    full_name: str


class Genre(BaseModel):
    id: str
    name: str


class FilmByPersonModel(BaseModel):
    id: str
    title: str
    imdb_rating: Optional[float] = None


class PersonFilmWork(BaseModel):
    id: str
    name: str


class ListResponseModel(BaseModel):
    total_count: int
    current_from: int
    page_size: int
    records: Union[List[Person], List[Genre], List[FilmByPersonModel]]


class FilmRespModel(BaseModel):
    id: str
    imdb_rating: float
    genres: Optional[list] = Field(None, alias='genre')
    title: str
    description: Optional[str] = None
    director: Optional[str] = None
    actors: Optional[List[PersonFilmWork]] = None
    writers: Optional[List[PersonFilmWork]] = None


class FilmsResponseModel(BaseModel):
    total_count: int
    page_count: int
    page_number: int
    page_size: int
    records: List[FilmRespModel]
