import logging
from functools import lru_cache
from typing import Any

from elasticsearch import AsyncElasticsearch, NotFoundError
from fastapi import Depends
from redis.asyncio import Redis

from src.db.elastic import get_elastic
from src.db.redis import get_redis
from src.models.film import FilmsDetailsResponseModel, FilmsResponseModel

FILM_CACHE_EXPIRE_IN_SECONDS = 60 * 5  # 5 минут
logger = logging.getLogger(__name__)


class FilmService:
    def __init__(self, redis: Redis, elastic: AsyncElasticsearch):
        self.redis = redis
        self.elastic = elastic

    async def get_by_id(self, film_id: str) -> FilmsDetailsResponseModel | None:
        film = await self._film_from_cache(film_id)
        if not film:
            film = await self._get_film_from_elastic(film_id)
            if not film:
                return None
            await self._put_film_to_cache(film)
        return film

    async def _get_film_from_elastic(self, film_id: str) -> FilmsDetailsResponseModel | None:
        try:
            doc = await self.elastic.get(index='movies', id=film_id)
            logger.info(f"Elasticsearch response: {doc}")

            film_data = doc['_source'].copy()
            film_data['id'] = doc['_id']

            if film_data.get('description') is None:
                film_data['description'] = ""

            return FilmsDetailsResponseModel(**film_data)
        except NotFoundError:
            logger.warning(f"Film {film_id} not found in Elasticsearch")
            return None
        except Exception as e:
            logger.error(f"Error getting film from Elasticsearch: {e}")
            return None

    async def _film_from_cache(self, film_id: str) -> FilmsDetailsResponseModel | None:
        data = await self.redis.get(film_id)
        if not data:
            return None

        try:
            film = FilmsDetailsResponseModel.model_validate_json(data)
            return film
        except Exception as e:
            logger.error(f"Error parsing film from cache: {e}")
            return None

    async def _put_film_to_cache(self, film: FilmsDetailsResponseModel):
        try:
            await self.redis.set(
                film.id,
                film.model_dump_json(),
                FILM_CACHE_EXPIRE_IN_SECONDS
            )
        except Exception as e:
            logger.error(f"Error putting film to cache: {e}")


    def _calculate_pagination(self, page_number: int, page_size: int) -> int:
        """Расчет индекса начала выборки"""
        return (page_number - 1) * page_size

    def _build_sort(self, sort: str) -> list[dict]:
        """Построение параметров сортировки"""
        sort_field = sort.lstrip('-')
        sort_order = "desc" if sort.startswith('-') else "asc"

        return [
            {
                sort_field: {
                    "order": sort_order
                }
            }
        ]

    def _build_search_body(
        self,
        page_size: int,
        from_index: int,
        query: dict[str, Any] | None = None,
        sort: list[dict] | None = None

    ) -> dict[str, Any]:
        """Построение тела запроса для Elasticsearch"""
        search_body = {
            "size": page_size,
            "from": from_index,
        }

        if query:
            search_body["query"] = query
        else:
            search_body["query"] = {"match_all": {}}

        if sort:
            search_body["sort"] = sort

        return search_body

    async def _process_elasticsearch_result(self, result: dict[str, Any]) -> list[FilmsResponseModel]:
        """Обработка результатов из Elasticsearch"""
        films = []
        for hit in result['hits']['hits']:
            film_data = hit['_source'].copy()
            film_data['id'] = hit['_id']

            film_response_data = {
                'id': film_data['id'],
                'title': film_data.get('title', ''),
                'imdb_rating': film_data.get('imdb_rating')
            }

            films.append(FilmsResponseModel(**film_response_data))

        return films

    async def _execute_elasticsearch_search(self, search_body: dict[str, Any]) -> list[FilmsResponseModel]:
        """Выполнение поиска в Elasticsearch"""
        try:
            result = await self.elastic.search(
                index="movies",
                body=search_body
            )
            return await self._process_elasticsearch_result(result)
        except Exception as e:
            logger.error(f"Error executing Elasticsearch search: {e}")
            return []

    async def get_films_list(
            self,
            sort: str = "-imdb_rating",
            page_size: int = 50,
            page_number: int = 1
    ) -> list[FilmsResponseModel]:
        """Получить список фильмов с пагинацией и сортировкой"""
        from_index = self._calculate_pagination(page_number, page_size)
        sort_body = self._build_sort(sort)
        search_body = self._build_search_body(page_size, from_index, sort=sort_body)

        return await self._execute_elasticsearch_search(search_body)

    async def get_search_films(
            self,
            query: str,
            page_size: int = 50,
            page_number: int = 1
    ) -> list[FilmsResponseModel]:
        """Поиск фильмов по названию"""
        from_index = self._calculate_pagination(page_number, page_size)
        search_query = {"match": {"title": query}}
        search_body = self._build_search_body(page_size, from_index, query=search_query)

        return await self._execute_elasticsearch_search(search_body)


@lru_cache()
def get_film_service(
        redis: Redis = Depends(get_redis),
        elastic: AsyncElasticsearch = Depends(get_elastic),
) -> FilmService:
    return FilmService(redis, elastic)