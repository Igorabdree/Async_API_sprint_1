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
                # self.state.state['page_size'],
            )

    def set_state(self, **kwargs) -> None:
        """установить состояние в кеш.

        Args:
            kwargs: Пара ключ/значение для сохранения в кэше.

        """
        for key, value in kwargs.items():
            self.state.set_state(key=key, value=value)

    def process(self, where_clause_table: str, pkeys: list) -> None:
        """Запустите sql для обогащения данных и передачи результатов в result_handler

        Args:
            where_clause_table: Имя таблицы
            pkeys: Первичные ключи для условий SQL

        """

        logger.debug('Выберите все данные о фильмах по %s', where_clause_table)

        query = SQL(sql.get_movie_info_by_id).format(
            where_clause_table=Identifier(where_clause_table),
        )

        while query_result := self.pg.retry_fetchall(
                query,
                pkeys=tuple(pkeys),
                last_id=self.state.get_state('last_processed_id') or '',
                page_size=self.page_size,
        ):
            self.set_state(
                table=where_clause_table,
                pkeys=pkeys,
                last_processed_id=query_result[-1]['id'],
                page_size=self.page_size,
            )
            logger.debug('Есть дополнительная информация о фильмах %s', len(query_result))
            self.result_handler(query_result)

        self.set_state(
            table=None,
            pkeys=None,
            last_processed_id=None,
            page_size=None,
        )
