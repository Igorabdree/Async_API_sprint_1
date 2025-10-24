from contextlib import asynccontextmanager
from elasticsearch import AsyncElasticsearch
from fastapi import FastAPI
from fastapi.responses import ORJSONResponse
from redis.asyncio import Redis
import uvicorn
import logging

from src.api.v1 import films, genres
from src.core import config, logger
from src.db import elastic, redis


@asynccontextmanager
async def lifespan(application: FastAPI):
    # Используем новый конфиг через settings
    redis.redis = Redis(
        host=config.settings.redis_host,
        port=config.settings.redis_port
    )
    elastic.es = AsyncElasticsearch(
        hosts=[config.settings.elastic_url]  # Используем computed property с полным URL
    )

    try:
        await redis.redis.ping()
        logging.info("✅ Redis connection established")

        if await elastic.es.ping():
            logging.info("✅ Elasticsearch connection established")
        else:
            logging.error("❌ Elasticsearch connection failed")

    except Exception as e:
        logging.error(f"❌ Connection error during startup: {e}")

    yield

    await redis.redis.close()
    await elastic.es.close()
    logging.info("✅ All connections closed")


app = FastAPI(
    title=config.settings.project_name,  # Используем новый конфиг
    docs_url='/api/openapi',
    openapi_url='/api/openapi.json',
    default_response_class=ORJSONResponse,
    lifespan=lifespan,
)

app.include_router(films.router, prefix='/api/v1/films', tags=['films'])
app.include_router(genres.router, prefix='/api/v1/genres', tags=['genres'])

if __name__ == '__main__':
    uvicorn.run(
        'main:app',
        host='0.0.0.0',
        port=8000,
        log_config=logger.LOGGING,
        log_level=logging.DEBUG,
    )