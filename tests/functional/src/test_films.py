import json

import pytest

from tests.conftest import es_write_data, make_get_request, es_write_full_data, load_es_data, es_client,setup_es_index, redis_client
from tests.functional.utils.helpers import Fixture


class TestFilms:

    @pytest.mark.parametrize(
        'query_params, expected_answer',
        [
            (
                    {'query': 'The Star', 'page_size': 100},
                    {'status': 200, 'length': 100}
            ),
            (
                    {'query': 'The Star', 'page_size': 1},
                    {'status': 200, 'length': 1}
            ),
            (
                    {'query': 'Mashed potato'},
                    {'status': 404, 'length': 1}
            ),
            (
                    {'query': 'Th'},
                    {'status': 404, 'length': 1}
            ),
            (
                    {'query': 'The', 'page_size': 101},
                    {'status': 422, 'length': 1}
            ),
            (
                    {'query': 'Mashed potato', 'page_size': 0},
                    {'status': 422, 'length': 1}
            )
        ]
    )
    @pytest.mark.asyncio
    async def test_search_films_on_page_size(
            self,
            es_write_data: Fixture,
            make_get_request:Fixture,
            query_params:dict,
            expected_answer: dict,
            load_es_data,
            es_client,
            setup_es_index
    ):

        await es_write_data()

        response = await make_get_request(
            path='/search/',
            query_data=query_params

        )

        assert response['status'] == expected_answer['status']
        if expected_answer['status'] ==200:
            assert len(response['body']['films']) == expected_answer['length']
        else:
            assert len(response['body']) == expected_answer['length']

    @pytest.mark.parametrize(
        'query_params, expected_answer',
        [
            (
                    {'sort': '-imdb_rating'},
                    {'status': 200, 'length': 50}
            ),
            (
                    {'sort': 'imdb_rating'},
                    {'status': 200, 'length': 50}
            ),
            (
                    {'sort': 'qwerty'},
                    {'status': 404, 'length': 1}
            ),
        ]
    )
    @pytest.mark.asyncio
    async def test_films(
            self,
            es_write_data: Fixture,
            make_get_request:Fixture,
            query_params:dict,
            expected_answer: dict,
            load_es_data: Fixture,
            es_client: Fixture,
            setup_es_index: Fixture
    ):

        await es_write_data()

        response = await make_get_request(
            query_data=query_params,
            path='/'

        )

        assert response['status'] == expected_answer['status']
        if expected_answer['status'] ==200:
            assert len(response['body']['films']) == expected_answer['length']
        else:
            assert len(response['body']) == expected_answer['length']


    @pytest.mark.parametrize(
        'path, rating,genres, expected_answer',
        [
            (
                'ef86b8ff-3c82-4d31-ad8e-72b69f4e3f95',
                1,
                "Horror",
                {'status': 200, 'length': 8}

            ),
            (
                'fb111f22-121e-44a7-b78f-b19191810fbf',
                10,
                "Comedy",
                {'status': 200, 'length': 8}

            ),
            (
                'fb111f22-121e-44a7-b78f-b19191810fbd',
                0,
                "Comedy",
                {'status': 404, 'length': 0}

            ),
        ]
    )
    @pytest.mark.asyncio
    async def test_film_id(
            self,
            es_write_full_data: Fixture,
            make_get_request: Fixture,
            load_es_data: Fixture,
            es_client: Fixture,
            setup_es_index: Fixture,
            expected_answer: dict,
            path: str,
            rating:int,
            genres: str

    ):

        await es_write_full_data()

        response = await make_get_request(
            path=f'/{path}'

        )

        assert response['status'] == expected_answer['status']
        print(response)
        if expected_answer['status'] ==200:
            assert len(response['body']) == expected_answer['length']
            assert response['body']["imdb_rating"] == rating
            assert response['body']["genres"][0] == genres





    @pytest.mark.parametrize(
        'path, rating,genres, expected_answer',
        [
            (
                'ef86b8ff-3c82-4d31-ad8e-72b69f4e3f95',
                1,
                "Horror",
                {'status': 200}
            ),
            (
                'fb111f22-121e-44a7-b78f-b19191810fbf',
                10,
                "Comedy",
                {'status': 200}
            ),

        ]
    )
    @pytest.mark.asyncio
    async def test_film_cache(
            self,
            es_write_full_data: Fixture,
            make_get_request: Fixture,
            load_es_data: Fixture,
            redis_client: Fixture,
            setup_es_index: Fixture,
            expected_answer: dict,
            path: str,
            rating:int,
            genres: str

    ):

        await es_write_full_data()

        response = await make_get_request(
            path=f'/{path}'

        )

        assert response['status'] == expected_answer['status']
        cached_data = await redis_client.get(path)

        assert cached_data is not None
        if cached_data:
            movie_data = json.loads(cached_data)
            assert movie_data['id'] == path
            assert 'title' in movie_data
            assert 'imdb_rating' in movie_data