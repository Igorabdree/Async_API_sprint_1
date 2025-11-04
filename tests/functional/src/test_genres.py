import json

import pytest

from conftest import es_write_data, make_get_genres_request, es_write_genres_data, load_es_data, es_client,setup_es_index, es_write_genres_full_data, redis_client

from functional.utils.helpers import Fixture


class TestGenres:

    @pytest.mark.parametrize(
        'expected_answer',
        [
            (
                {'status': 200, 'length': 20, 'length_genre': 2}
            ),
        ]
    )
    @pytest.mark.asyncio
    async def test_get_genres(
            self,
            es_write_data: Fixture,
            expected_answer: dict,
            load_es_data: Fixture,
            es_client: Fixture,
            setup_es_index: Fixture,
            es_write_genres_data: Fixture,
            make_get_genres_request: Fixture,
    ):

        await es_write_genres_data()

        response = await make_get_genres_request(
            path='/'

        )

        assert response['status'] == expected_answer['status']
        assert len(response['body']) == expected_answer['length']
        assert len(response['body'][0]) == expected_answer['length_genre']


    @pytest.mark.parametrize(
        'path, expected_answer',
        [
            (
                '3d8d9bf5-0d90-4353-88ba-4ccc5d2c07ff',
                {'status': 200, 'length': 5}
            ),
            (
                '3d8d9bf5-0d90-4353-88ba-4ccc5d2c',
                {'status': 404, 'length': 5}
            ),
        ]
    )
    @pytest.mark.asyncio
    async def test_get_genres_on_id(
        self,
        es_write_data: Fixture,
        expected_answer: dict,
        load_es_data: Fixture,
        es_client: Fixture,
        setup_es_index: Fixture,
        es_write_genres_full_data: Fixture,
        make_get_genres_request: Fixture,
        path: str
    ):

        await es_write_genres_full_data()

        response = await make_get_genres_request(
            path=f'/{path}'

        )

        assert response['status'] == expected_answer['status']
        if response['status'] == 200:
            assert len(response['body']) == expected_answer['length']
            assert len(response['body']["film_ids"]) == len(response['body']["film_titles"])
            assert len(response['body']["film_ids"]) == len(response['body']["film_titles"])
            assert len(response['body']["film_titles"]) == response['body']["films_count"]




    @pytest.mark.parametrize(
        'path, expected_answer',
        [
            (
                    '3d8d9bf5-0d90-4353-88ba-4ccc5d2c07ff',
                    {'status': 200}
            ),
        ]
    )
    @pytest.mark.asyncio
    async def test_get_genres_cache(
            self,
            es_write_data: Fixture,
            expected_answer: dict,
            load_es_data: Fixture,
            es_client: Fixture,
            setup_es_index: Fixture,
            es_write_genres_full_data: Fixture,
            make_get_genres_request: Fixture,
            redis_client: Fixture,
            path: str
    ):

        await es_write_genres_full_data()

        response = await make_get_genres_request(
            path=f'/{path}'
        )

        assert response['status'] == expected_answer['status']
        cached_data = await redis_client.get(path)
        assert cached_data is not None
        print(cached_data)
        if cached_data:
            genres_data = json.loads(cached_data)
            assert genres_data['id'] == path
            assert 'film_titles' in genres_data
            assert 'films_count' in genres_data