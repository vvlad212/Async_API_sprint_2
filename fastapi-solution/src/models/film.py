from typing import List, Union

from models.config import Base
from pydantic import Field, validator
from pydantic.class_validators import Optional


class FilmFull(Base):
    genre: Optional[list] = None
    title: str = None
    description: Optional[str] = None
    director: Optional[Union[str, List[str]]] = None
    actors_names: Optional[Union[str, List[str]]] = None
    writers_names: Optional[List[str]] = None
    actors: Optional[List[dict]] = None
    writers: Optional[List[dict]] = None
    imdb_rating: Optional[float] = Field(
        ge=0,
        le=10,
    )
    id: str

    @validator('imdb_rating')
    def set_imdb_rating(cls, rating):
        return rating or 0
