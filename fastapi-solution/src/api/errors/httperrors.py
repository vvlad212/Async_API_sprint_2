from http import HTTPStatus

from fastapi import HTTPException

GenreHTTPNotFoundError = HTTPException(status_code=HTTPStatus.NOT_FOUND, detail='Genre(s) not found')
FilmHTTPNotFoundError = HTTPException(status_code=HTTPStatus.NOT_FOUND, detail='Film(s) not found')
PersonHTTPNotFoundError = HTTPException(status_code=HTTPStatus.NOT_FOUND, detail='Person(s) not found')
