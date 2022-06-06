from fastapi import Query
from pydantic import BaseModel


class Paginator(BaseModel):
    page_size: int
    page_number: int


async def parse_pagination(
        page_size: int = Query(
            default=20,
            gt=0,
            title="Page size",
            description="Number of posts per page.",
            alias="page[size]"),

        page_number: int = Query(
            default=0,
            gt=0,
            title="Page number",
            description="Pagination page number.",
            alias="page[number]"
        )
) -> Paginator:
    return Paginator(page_size=page_size, page_number=page_number)
