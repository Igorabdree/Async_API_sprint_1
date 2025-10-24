from functools import lru_cache
import logging

from elasticsearch import AsyncElasticsearch, NotFoundError
from fastapi import Depends
from redis.asyncio import Redis

from src.db.elastic import get_elastic
from src.db.redis import get_redis
from src.models.genres import  GenresFullResponse

FILM_CACHE_EXPIRE_IN_SECONDS = 60 * 5  # 5 минут
logger = logging.getLogger(__name__)


class GenresService:
    def __init__(self, redis: Redis, elastic: AsyncElasticsearch):
        self.redis = redis
        self.elastic = elastic

    async def get_by_id(self, genres_id: str) -> GenresFullResponse | None:
        genres = await self._genres_from_cache(genres_id)
        if not genres:
            genres = await self._get_genres_from_elastic(genres_id)
            if not genres:
                return None
            await self._put_genres_to_cache(genres)
        return genres

    async def _get_genres_from_elastic(self, genres_id: str) ->  GenresFullResponse | None:
        try:
            doc = await self.elastic.get(index='genres', id=genres_id)
            logger.info(f"Elasticsearch response: {doc}")

            genres_data = doc['_source'].copy()
            genres_data['id'] = doc['_id']


            return GenresFullResponse(**genres_data)
        except NotFoundError:
            logger.warning(f"genres {genres_id} not found in Elasticsearch")
            return None
        except Exception as e:
            logger.error(f"Error getting genres from Elasticsearch: {e}")
            return None


    async def _genres_from_cache(self, genres_id: str) -> GenresFullResponse | None:
        data = await self.redis.get(genres_id)
        if not data:
            return None

        try:
            genres = GenresFullResponse.model_validate_json(data)
            return genres
        except Exception as e:
            logger.error(f"Error parsing film from cache: {e}")
            return None

    async def _put_genres_to_cache(self, genres: GenresFullResponse):
        try:
            await self.redis.set(
                genres.id,
                genres.model_dump_json(),
                FILM_CACHE_EXPIRE_IN_SECONDS
            )
        except Exception as e:
            logger.error(f"Error putting genres to cache: {e}")


@lru_cache()
def get_film_service(
        redis: Redis = Depends(get_redis),
        elastic: AsyncElasticsearch = Depends(get_elastic),
) -> GenresService:
    return GenresService(redis, elastic)