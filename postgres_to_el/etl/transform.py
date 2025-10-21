from typing import Callable

import logging
from logging.config import dictConfig
from schema import Movie, Person

from lib.loggers import LOGGING
from states import RedisStorage, State

dictConfig(LOGGING)
logger = logging.getLogger(__name__)

class Transform:

    def __init__(self, result_handler: Callable, redis_settings: dict):

        """Конструктор класса Transform

        Args:
            result_handler: Результат обработки
            redis_settings: Redis connection

        """

        self.result_handler = result_handler
        self.storage = RedisStorage(redis_settings=redis_settings)
        self.state = State(self.storage)
        self.proceed()

    def proceed(self):
        """Проверьте состояние и продолжить"""

        if self.state.state.get('data'):
            logger.debug('Данные для продолжения %s', self.state.state.get('data'))
            self.process(
                movies=self.state.state.get('data'),
            )

    def set_state(self, **kwargs):
        """Установить состояние"""
        for key, value in kwargs.items():
            self.state.set_state(key=key, value=value)

    def get_person_name(self, persons: dict, roles: list = None):
        """Получить список имен людей"""
        if not persons:
            return []

        result = []
        for i, person in enumerate(persons):
            full_name = person.get('name')
            person_role = person.get('role')

            if not full_name:
                continue

            if roles and person_role in roles:
                result.append(full_name)
            elif not roles:
                result.append(full_name)
        return result

    def get_person_name_schema(self, persons: dict, role: list = None):
        """Получить список людей"""

        if not persons:
            return []

        filtered_persons = []
        for i, person in enumerate(persons):
            person_id = person.get('id')
            full_name = person.get('name')
            person_role = person.get('role')


            if not person_id or not full_name:
                continue

            if not role or person_role in role:
                person_data = {
                    'id': person_id,
                    'name': full_name
                }
                filtered_persons.append(person_data)

        result = [Person(**person) for person in filtered_persons]

        return result



    def process(self, movies: list):
        """Преобразовать данные и передать в result_handler."""

        for idx, movie in enumerate(movies):

            try:
                persons = movie.get('persons', [])
                genres = movie.get('genre', [])

                transformed_movie = Movie(
                    id=movie.get('id', ''),
                    imdb_rating=movie.get('imdb_rating'),
                    genres=genres,  # ИСПРАВЛЕНО
                    title=movie.get('title', ''),
                    description=movie.get('description', ''),
                    directors_names=self.get_person_name(persons, ['director']),
                    actors_names=self.get_person_name(persons, ['actor']),
                    writers_names=self.get_person_name(persons, ['writer']),
                    directors=self.get_person_name_schema(persons, ['director']),
                    actors=self.get_person_name_schema(persons, ['actor']),
                    writers=self.get_person_name_schema(persons, ['writer']),
                )

                result_data = transformed_movie.model_dump(by_alias=True)

                for field in ['directors', 'actors', 'writers', 'genres',
                              'directors_names', 'actors_names', 'writers_names']:
                    if result_data.get(field) is None:
                        result_data[field] = []

                movies[idx] = result_data

            except Exception as e:
                logger.exception('Validation data error for movie %s: %s', movie.get('id', 'unknown'), e)
                continue

        self.set_state(data=None)
        self.result_handler(movies)
