import random
from tests.functional.settings import test_settings
from elasticsearch import AsyncElasticsearch
from elasticsearch.helpers import async_bulk
import pytest_asyncio
import asyncio
import datetime
import uuid
import aiohttp
import pytest
import pytest_asyncio
import uuid
import random
import datetime


@pytest_asyncio.fixture(scope='session')
def event_loop():
    loop = asyncio.get_event_loop()
    yield loop
    loop.close()

# 1. Фикстура для работы с ES клиентом
@pytest_asyncio.fixture
async def es_client():
    """Фикстура для работы с Elasticsearch клиентом"""
    es_client = AsyncElasticsearch(hosts=test_settings.es_host, verify_certs=False)
    try:
        yield es_client
    finally:
        await es_client.close()

# # 2. Фикстура для очистки и создания индекса
# @pytest_asyncio.fixture
# async def setup_es_index(es_client):
#     """Фикстура для очистки и создания индекса"""
#     # Очищаем индекс
#     if await es_client.indices.exists(index=test_settings.es_index):
#         await es_client.indices.delete(index=test_settings.es_index)
#
#     # Создаем индекс
#     await es_client.indices.create(
#         index=test_settings.es_index,
#         **test_settings.es_index_mapping
#     )

@pytest_asyncio.fixture
async def setup_es_index(es_client):
    """Фикстура для очистки и создания индекса с параметром"""
    async def inner(index_name: str):
        # Очищаем индекс
        if await es_client.indices.exists(index=index_name):
            await es_client.indices.delete(index=index_name)
            await asyncio.sleep(0.1)

        # Создаем индекс
        await es_client.indices.create(
            index=index_name,
            **test_settings.es_index_mapping
        )
        print(f"✅ Index '{index_name}' recreated")

    return inner


# # 3. Фикстура для загрузки данных в ES
# @pytest_asyncio.fixture
# async def load_es_data(es_client, setup_es_index):
#     """Фикстура для загрузки данных в Elasticsearch"""
#     async def inner(bulk_query: list[dict]):
#         updated, errors = await async_bulk(
#             client=es_client,
#             actions=bulk_query,
#             refresh=True
#         )
#         if errors:
#             raise Exception('Ошибка записи данных в Elasticsearch')
#         return updated
#     return inner

@pytest_asyncio.fixture
async def load_es_data(es_client, setup_es_index):
    """Фикстура для загрузки данных в Elasticsearch"""
    async def inner(bulk_query: list[dict], index_name: str = None):
        # Если индекс не указан, берем из первого документа
        if index_name is None and bulk_query:
            index_name = bulk_query[0]['_index']
        elif index_name is None:
            index_name = test_settings.es_index

        # Настраиваем индекс
        await setup_es_index(index_name)

        # Загружаем данные
        updated, errors = await async_bulk(
            client=es_client,
            actions=bulk_query,
            refresh=True
        )

        if errors:
            raise Exception('Ошибка записи данных в Elasticsearch')
        return updated

    return inner


# 4. Фикстура для генерации тестовых данных (120 фильмов)
@pytest_asyncio.fixture
async def es_write_data(load_es_data):
    async def inner():
        es_data = [{
            'id': str(uuid.uuid4()),
            'imdb_rating': round(random.uniform(1.0, 10.0), 1),
            'title': 'The Star',
            'created_at': datetime.datetime.now().isoformat(),
            'updated_at': datetime.datetime.now().isoformat(),
            'film_work_type': 'movie'
        } for _ in range(120)]

        bulk_query = []
        for row in es_data:
            bulk_query.append({'_index': 'movies_test', '_id': row['id'], '_source': row})

        # Загружаем данные используя общую фикстуру
        await load_es_data(bulk_query)

    return inner

# 5. Фикстура для генерации полных тестовых данных (2 фильма)
@pytest_asyncio.fixture
async def es_write_full_data(load_es_data):
    async def inner():
        film_data = [
            {
                'id': 'ef86b8ff-3c82-4d31-ad8e-72b69f4e3f95',
                'imdb_rating': 1.0,
                'genres': ['Horror'],
                'title': 'Want'
            },
            {
                'id': 'fb111f22-121e-44a7-b78f-b19191810fbf',
                'imdb_rating': 10.0,
                'genres': ['Comedy'],
                'title': 'Lot'
            }
        ]

        es_data = []
        for film in film_data:
            es_data.append({
                'id': film['id'],
                'imdb_rating': film['imdb_rating'],
                'genres': film['genres'],
                'title': film['title'],
                'description': 'New World',
                'directors': ['Stan'],
                'actors_names': ['Ann', 'Bob'],
                'writers_names': ['Ben', 'Howard'],
                'actors': [
                    {'id': 'ef86b8ff-3c82-4d31-ad8e-72b69f4e3f95', 'name': 'Ann'},
                    {'id': 'fb111f22-121e-44a7-b78f-b19191810fbf', 'name': 'Bob'}
                ],
                'writers': [
                    {'id': 'caf76c67-c0fe-477e-8766-3ab3ff2574b5', 'name': 'Ben'},
                    {'id': 'b45bd7bc-2e16-46d5-b125-983d356768c6', 'name': 'Howard'}
                ],
                'created_at': datetime.datetime.now().isoformat(),
                'updated_at': datetime.datetime.now().isoformat(),
                'film_work_type': 'movie'
            })

        bulk_query = []
        for row in es_data:
            bulk_query.append({'_index': 'movies_test', '_id': row['id'], '_source': row})

        # Загружаем данные используя общую фикстуру
        await load_es_data(bulk_query, index_name='movies_test')

    return inner

# 6. Фикстура для HTTP запросов (остается без изменений)
@pytest_asyncio.fixture
async def make_get_request():
    async def inner(path: str, query_data: dict = None):
        async with aiohttp.ClientSession() as session:
            url = f'http://localhost:8000/api/v1/films{path}'
            async with session.get(url, params=query_data) as response:
                return {
                    'body': await response.json(),
                    'headers': dict(response.headers),
                    'status': response.status
                }
    return inner

@pytest_asyncio.fixture
async def make_get_genres_request():
    async def inner(path: str, query_data: dict = None):
        async with aiohttp.ClientSession() as session:
            url = f'http://localhost:8000/api/v1/genres{path}'
            async with session.get(url, params=query_data) as response:
                return {
                    'body': await response.json(),
                    'headers': dict(response.headers),
                    'status': response.status
                }
    return inner


@pytest_asyncio.fixture
async def es_write_genres_data(load_es_data):
    """Фикстура для загрузки тестовых данных жанров в Elasticsearch"""
    async def inner():
        # Список реальных жанров
        real_genres = [
            "Action", "Adventure", "Animation", "Comedy", "Crime",
            "Documentary", "Drama", "Fantasy", "Horror", "Mystery",
            "Romance", "Science Fiction", "Thriller", "Western",
            "Family", "Musical", "War", "History", "Biography", "Sport"
        ]

        # Генерируем данные жанров
        es_data = [{
            'id': str(uuid.uuid4()),
            'name': genre_name,
            # 'description': f"Фильмы в жанре {genre_name}",
            # 'created': datetime.now().isoformat(),
            # 'updated': datetime.now().isoformat(),
            # 'film_count': random.randint(10, 500)
        } for genre_name in real_genres]

        # Подготавливаем bulk запрос
        bulk_query = []
        for row in es_data:
            bulk_query.append({
                '_index': 'genres_test',
                '_id': row['id'],
                '_source': row
            })

        # Загружаем данные используя общую фикстуру
        # await load_es_data(bulk_query)
        await load_es_data(bulk_query, index_name='genres_test')

    return inner


@pytest_asyncio.fixture
async def es_write_genres_full_data(load_es_data):
    """Фикстура для загрузки тестовых данных жанров в Elasticsearch"""
    async def inner():
        # Список реальных жанров с фиксированными UUID
        real_genres = [
            {"name": "Action", "id": "3d8d9bf5-0d90-4353-88ba-4ccc5d2c07ff"},
            {"name": "Adventure", "id": "120a21cf-9097-479e-904a-13dd7198c1dd"},
            {"name": "Animation", "id": "b92ef010-5e4c-4fd0-99d6-41b6456272cd"},
            {"name": "Comedy", "id": "6c162475-c7ed-4461-9184-001ef3d9f26e"},
            {"name": "Crime", "id": "1cacff68-643e-4ddd-93f4-5f297081eaa8"},
            {"name": "Documentary", "id": "0b105f87-e0a5-45dc-8ce7-f8632088f390"},
            {"name": "Drama", "id": "237fd1e4-c98e-454e-aa0e-653e6c4c5e5a"},
            {"name": "Fantasy", "id": "526769d7-df18-4661-9aa6-5ed1011121a8"},
            {"name": "Horror", "id": "56b541ab-4d66-4021-8708-397762bff2d4"},
            {"name": "Mystery", "id": "ca88141b-a6b4-450d-bbc3-efa940e4953f"},
            {"name": "Romance", "id": "5373d043-3f41-4ea8-9947-4c7d30c94a15"},
            {"name": "Science Fiction", "id": "6a0a479b-cfec-41ac-b520-41b2b007b611"},
            {"name": "Thriller", "id": "c020dab2-e9bd-4c4a-b2c4-59c7f7a4b7d1"},
            {"name": "Western", "id": "63c24835-34d3-4279-8d81-3c5f4ddb0cdc"},
            {"name": "Family", "id": "55c723c1-6d5f-402f-b147-4b63d8f0d8b3"},
            {"name": "Musical", "id": "c7e9b8f2-4a1d-4e3a-9b5c-8d8e8f9a0b1c"},
            {"name": "War", "id": "d8e9f0a1-5b2c-4d3e-8f9a-1b2c3d4e5f6a"},
            {"name": "History", "id": "e9f0a1b2-6c3d-4e4f-9a0b-2c3d4e5f6a7b"},
            {"name": "Biography", "id": "f0a1b2c3-7d4e-4f5a-0b1c-3d4e5f6a7b8c"},
            {"name": "Sport", "id": "a1b2c3d4-8e5f-4a6b-1c2d-4e5f6a7b8c9d"}
        ]

        # Список популярных фильмов для заполнения
        popular_films = [
            "The Matrix", "Inception", "Avatar", "Titanic", "Star Wars",
            "The Godfather", "Pulp Fiction", "Forrest Gump", "The Dark Knight",
            "Fight Club", "The Shawshank Redemption", "Jurassic Park",
            "The Lord of the Rings", "Harry Potter", "The Avengers",
            "Spider-Man", "Iron Man", "Black Panther", "Wonder Woman", "Joker"
        ]

        # Генерируем данные жанров по схеме
        es_data = []
        for genre in real_genres:
            import random
            films_count = random.randint(3, 8)
            film_titles = random.sample(popular_films, films_count)
            film_ids = [str(uuid.uuid4()) for _ in range(films_count)]

            es_data.append({
                'id': genre['id'],  # используем фиксированный UUID
                'name': genre['name'],
                'film_titles': film_titles,
                'film_ids': film_ids,
                'films_count': films_count
            })

        # Подготавливаем bulk запрос
        bulk_query = []
        for row in es_data:
            bulk_query.append({
                '_index': 'genres_test',
                '_id': row['id'],  # используем фиксированный UUID как ID документа
                '_source': row
            })

        # Загружаем данные используя общую фикстуру
        await load_es_data(bulk_query, index_name='genres_test')

    return inner

import pytest_asyncio
from redis.asyncio import Redis
import os

import pytest
import pytest_asyncio
from redis.asyncio import Redis
import os

@pytest_asyncio.fixture
async def redis_client():
    """Асинхронная фикстура для клиента Redis"""
    redis = Redis(
        host='127.0.0.1',
        port=6379,
        db=0,
        decode_responses=True
    )

    try:
        # Очищаем БД перед тестом
        await redis.flushdb()
        yield redis
    finally:
        # Очищаем БД после теста и закрываем соединение
        await redis.flushdb()
        await redis.close()