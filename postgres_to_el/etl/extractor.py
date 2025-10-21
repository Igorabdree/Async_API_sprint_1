import datetime
import logging
from logging.config import dictConfig
from typing import Callable


from database.pg_database import PGConnection
from lib.loggers import LOGGING
from lib import sql
from psycopg2.sql import SQL, Identifier
from states import RedisStorage, State
from datetime import date

dictConfig(LOGGING)
logger = logging.getLogger(__name__)


class Extractor:

    def __init__(self, pg: PGConnection, redis_settings: dict, result_handler: Callable) -> None:
        """Конструктор класса Extractor.

        Args:
            pg: Данные для подключения к бд
            result_handler: Результат обработки
            redis_settings: Redis connection
        """
        self.pg = pg
        self.result_handler = result_handler
        self.storage = RedisStorage(redis_settings)
        self.state = State(self.storage)

    def get_last_modified(self, table: str) -> str:
        """ Получить последний id из кэша.

        Args:
            table: таблица
        """
        modified = self.state.get_state(table)

        if modified:
            return modified

        else:
            old_date = "1900-01-01T00:00:00"
            return old_date

    def process(self, table: str, schema: str = 'content', page_size: int = 100) -> None:
        """

        Получить измененные данные.

        Args:
            table: имя таблицы
            schema: Схема базы данных
            page_size: Количество записей

        """
        logger.debug('Выберите updated_at из %s', table)

        query = SQL(sql.get_modified_records).format(
            table=Identifier(schema, table),
        )

        query_result = self.pg.retry_fetchall(
            query,
            modified=self.get_last_modified(table),
            page_size=page_size,
        )

        logger.debug('Получено %s записей из таблицы %s', len(query_result), table)
        if query_result:
            modified = query_result[-1]['modified']

            def convert_to_serializable(obj):
                """Преобразует объект в сериализуемый формат."""
                if hasattr(obj, 'isoformat'):
                    return obj.isoformat()
                elif isinstance(obj, (datetime, date)):
                    return obj.isoformat()
                else:
                    return obj

            modified_serializable = convert_to_serializable(modified)

            self.state.set_state(key=table, value=modified_serializable)

            self.result_handler(
                where_clause_table=table,
                pkeys=[record['id'] for record in query_result],
            )
