import logging

import aioredis
import uvicorn
from elasticsearch import AsyncElasticsearch
from fastapi import FastAPI
from fastapi.openapi.utils import get_openapi
from fastapi.responses import ORJSONResponse

from api.metadata.tags_metadata import tags_metadata
from api.v1 import films, genre, person
from core import config
from core.logger import LOGGING
from db import elastic, redis

app = FastAPI(
    docs_url='/api/openapi',
    openapi_url='/api/openapi.json',
    default_response_class=ORJSONResponse,
)


def custom_openapi():
    if app.openapi_schema:
        return app.openapi_schema
    openapi_schema = get_openapi(
        title="Online cinema api",
        version="1.0.0",
        description="This is the first version of the cinema API made by Vladislav and Nick",
        routes=app.routes,
        tags=tags_metadata
    )
    app.openapi_schema = openapi_schema
    return app.openapi_schema


app.openapi = custom_openapi


@app.on_event('startup')
async def startup():
    redis.redis = await aioredis.create_redis_pool((config.REDIS_HOST, config.REDIS_PORT), minsize=10, maxsize=20)
    elastic.es = AsyncElasticsearch(
        hosts=[f'{config.ELASTIC_HOST}:{config.ELASTIC_PORT}'])


@app.on_event('shutdown')
async def shutdown():
    redis.redis.close()
    await redis.redis.wait_closed()
    await elastic.es.close()


# Подключаем роутер к серверу, указав префикс /v1/films
# Теги указываем для удобства навигации по документации
app.include_router(films.router, prefix='/api/v1/films')
app.include_router(person.router, prefix='/api/v1/person', tags=['person'])
app.include_router(genre.router, prefix='/api/v1/genre', tags=['genre'])

