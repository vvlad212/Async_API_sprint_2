"""
Класс моделей данных.

Описывает структуру таблиц
"""

import uuid
from typing import List, Optional

from pydantic import BaseModel


class FilmWorkPersonGenre(BaseModel):
    """Класс описывающий структуру таблицы Film_work."""

    genre: Optional[list] = None
    title: Optional[str] = None
    description: Optional[str] = None
    director: Optional[List[str]] = None
    actors_names: Optional[list] = None
    writers_names: Optional[list] = None
    actors: Optional[List[dict]] = None
    writers: Optional[List[dict]] = None
    imdb_rating: Optional[float] = None
    id: uuid.UUID


class Person(BaseModel):
    full_name: Optional[str] = None
    id: uuid.UUID


class Genre(BaseModel):
    name: Optional[str] = None
    id: uuid.UUID
