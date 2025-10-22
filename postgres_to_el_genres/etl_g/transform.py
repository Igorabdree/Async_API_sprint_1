from typing import Callable
import logging
from logging.config import dictConfig

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
                genres=self.state.state.get('data'),
            )

    def set_state(self, **kwargs):
        """Установить состояние"""
        for key, value in kwargs.items():
            self.state.set_state(key=key, value=value)

    def process(self, genres: list):
        """Преобразовать данные жанров и передать в result_handler."""

        transformed_genres = []

        for genre in genres:
            try:
                # Преобразуем данные жанра в нужный формат
                transformed_genre = {
                    'id': genre.get('id', ''),
                    'name': genre.get('name', ''),
                    'film_titles': genre.get('film_titles', []),
                    'films_count': genre.get('films_count', 0),
                    'film_ids': genre.get('film_ids', [])
                }

                # Валидация обязательных полей
                if not transformed_genre['id']:
                    logger.warning('Genre without ID: %s', genre)
                    continue

                if not transformed_genre['name']:
                    logger.warning('Genre without name: %s', genre.get('id'))
                    continue

                # Обеспечиваем правильные типы данных
                transformed_genre['films_count'] = int(transformed_genre['films_count'])

                # Убеждаемся, что film_ids - это список
                if not isinstance(transformed_genre['film_ids'], list):
                    transformed_genre['film_ids'] = []

                transformed_genres.append(transformed_genre)

            except Exception as e:
                logger.exception('Validation data error for genre %s: %s',
                                 genre.get('id', 'unknown'), e)
                continue

        # Сбрасываем состояние
        self.set_state(data=None)

        # Передаем преобразованные данные в обработчик
        if transformed_genres:
            self.result_handler(transformed_genres)
        else:
            logger.warning('No valid genres to process')
