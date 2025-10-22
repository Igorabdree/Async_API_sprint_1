import logging
from logging.config import dictConfig
from typing import Callable

from lib.loggers import LOGGING
from database.pg_database import PGConnection
from lib import sql
from psycopg2.sql import SQL, Identifier
from states import RedisStorage, State

dictConfig(LOGGING)
logger = logging.getLogger(__name__)


class Enricher(object):

    def __init__(self, pg: PGConnection, redis_settings: dict, result_handler: Callable, page_size: int = 100) -> None:
        """Конструктор класса Enricher.

        Args:
            pg: Данные для подключения к бд
            result_handler: Результат обработки
            redis_settings: Redis connection

        """
        self.pg = pg
        self.result_handler = result_handler
        self.storage = RedisStorage(redis_settings)
        self.state = State(self.storage)
        self.page_size = page_size
        self.proceed()

    def proceed(self) -> None:
        """Проверьте состояние и приступить к работе"""
        if self.state.state.get('pkeys'):
            logger.debug('Data to proceed %s', self.state.state.get('pkeys'))
            self.process(
                self.state.state['table'],
                self.state.state['pkeys'],
            )

    def set_state(self, **kwargs) -> None:
        """установить состояние в кеш.

        Args:
            kwargs: Пара ключ/значение для сохранения в кэше.

        """
        for key, value in kwargs.items():
            self.state.set_state(key=key, value=value)

    def convert_postgres_array_to_list(self, array_string):
        """Преобразовать строку массива PostgreSQL в Python список"""
        if array_string is None:
            return []

        if isinstance(array_string, list):
            return array_string

        if isinstance(array_string, str):
            # Убираем фигурные скобки и разделяем по запятым
            cleaned_string = array_string.strip('{}')
            if not cleaned_string:
                return []

            # Разделяем и очищаем элементы
            items = cleaned_string.split(',')
            result = [item.strip().strip('"') for item in items if item.strip()]
            return result

        # Если другой тип, пытаемся преобразовать в список
        try:
            return list(array_string)
        except:
            return []

    def process(self, where_clause_table: str, pkeys: list) -> None:
        """Запустите sql для обогащения данных и передачи результатов в result_handler

        Args:
            where_clause_table: Имя таблицы
            pkeys: Первичные ключи для условий SQL

        """

        logger.info('🎯 Выберите все данные о жанрах по %s', where_clause_table)

        # Используем SQL запрос из отдельного файла с модификациями для пагинации
        base_query = sql.get_genres_info_by_id

        # Добавляем WHERE условие для фильтрации по ID жанров
        if pkeys:
            query = base_query.replace(
                "GROUP BY g.id, g.pname",
                "WHERE g.id IN %(pkeys)s GROUP BY g.id, g.pname"
            )
        else:
            query = base_query

        # Добавляем пагинацию
        query += " LIMIT %(page_size)s OFFSET %(offset)s"

        offset = 0
        total_processed = 0

        while True:
            params = {
                'page_size': self.page_size,
                'offset': offset
            }

            if pkeys:
                params['pkeys'] = tuple(pkeys)

            query_result = self.pg.retry_fetchall(query, **params)

            if not query_result:
                logger.info('ℹ️ Больше нет данных для обработки')
                break

            # Сохраняем состояние обработки
            self.set_state(
                table=where_clause_table,
                pkeys=pkeys,
                last_processed_id=query_result[-1]['genre_id'],
                page_size=self.page_size,
                offset=offset
            )

            logger.info('📊 Получена информация о %s жанрах', len(query_result))

            # Преобразуем данные в нужный формат для ETL
            enriched_data = []
            for row in query_result:
                # Преобразуем film_ids из строки PostgreSQL в список
                film_ids = self.convert_postgres_array_to_list(row['film_ids'])
                film_titles = self.convert_postgres_array_to_list(row['film_titles'])

                enriched_genre = {
                    'id': row['genre_id'],
                    'name': row['genre_name'],
                    'films_count': row['films_count'],
                    'film_ids': film_ids,
                    'film_titles': film_titles
                }
                enriched_data.append(enriched_genre)

            # Логируем пример преобразованных данных
            if offset == 0 and enriched_data:
                logger.info("✅ ПРИМЕР ПРЕОБРАЗОВАННЫХ ДАННЫХ:")
                for i, genre in enumerate(enriched_data[:2]):
                    logger.info(f"  {i+1}. {genre['name']}:")
                    logger.info(f"     films_count: {genre['films_count']}")
                    logger.info(f"     film_ids type: {type(genre['film_ids'])}")
                    logger.info(f"     film_ids length: {len(genre['film_ids'])}")
                    logger.info(f"     Пример film_ids: {genre['film_ids'][:3]}")

            total_processed += len(enriched_data)
            logger.info(f'📤 Передача {len(enriched_data)} обогащенных записей в transform')

            # Передаем обогащенные данные в обработчик
            self.result_handler(enriched_data)

            # Увеличиваем offset для следующей страницы
            offset += self.page_size

            # Если получено меньше записей, чем размер страницы, значит это последняя страница
            if len(query_result) < self.page_size:
                logger.info(f'✅ Обработка завершена. Всего обработано: {total_processed} записей')
                break

        # Сбрасываем состояние после завершения обработки
        self.set_state(
            table=None,
            pkeys=None,
            last_processed_id=None,
            page_size=None,
            offset=None
        )

    def process_all_genres(self) -> None:
        """Метод для обработки всех жанров без фильтра по pkeys"""
        logger.info('🚀 Запуск обработки всех жанров')

        # Используем базовый запрос без WHERE условия
        query = sql.get_genres_info_by_id + " LIMIT %(page_size)s OFFSET %(offset)s"

        offset = 0
        total_processed = 0

        while True:
            query_result = self.pg.retry_fetchall(
                query,
                page_size=self.page_size,
                offset=offset
            )

            if not query_result:
                logger.info('ℹ️ Больше нет данных для обработки')
                break

            logger.info('📊 Получена информация о %s жанрах', len(query_result))

            enriched_data = []
            for row in query_result:
                # Преобразуем film_ids из строки PostgreSQL в список
                film_ids = self.convert_postgres_array_to_list(row['film_ids'])
                film_titles = self.convert_postgres_array_to_list(row['film_titles'])

                enriched_genre = {
                    'id': row['genre_id'],
                    'name': row['genre_name'],
                    'films_count': row['films_count'],
                    'film_ids': film_ids,
                    'film_titles': film_titles
                }
                enriched_data.append(enriched_genre)

            total_processed += len(enriched_data)
            logger.info(f'📤 Передача {len(enriched_data)} жанров в transform (всего: {total_processed})')

            self.result_handler(enriched_data)
            offset += self.page_size

            if len(query_result) < self.page_size:
                logger.info(f'✅ Обработка всех жанров завершена. Всего: {total_processed}')
                break