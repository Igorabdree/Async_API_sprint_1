
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

        logger.info(f"🔍 Получено last_modified для {table}: {modified}")

        if modified:
            return modified
        else:
            old_date = "1900-01-01T00:00:00"
            logger.info(f"🕐 Используем дату по умолчанию для {table}: {old_date}")
            return old_date

    def process(self, table: str, schema: str = 'content', page_size: int = 100) -> None:
        """

        Получить измененные данные.

        Args:
            table: имя таблицы
            schema: Схема базы данных
            page_size: Количество записей

        """
        logger.info(f'🚀 START: Выборка updated_at из {schema}.{table}')

        query = SQL(sql.get_modified_records).format(
            table=Identifier(schema, table),
        )

        last_modified = self.get_last_modified(table)
        logger.info(f"📅 Last modified для {table}: {last_modified}")
        logger.info(f"📝 SQL запрос: {query}")

        try:
            query_result = self.pg.retry_fetchall(
                query,
                modified=last_modified,
                page_size=page_size,
            )

            logger.info(f'✅ Получено {len(query_result)} записей из таблицы {table}')

            if query_result:
                # Показать первые 3 записи для отладки
                logger.info("📄 Пример записей:")
                for i, record in enumerate(query_result[:3]):
                    logger.info(f"  {i+1}. ID: {record['id']}, Modified: {record['modified']}")

                modified = query_result[-1]['modified']
                logger.info(f"🕒 Последняя модификация: {modified}")

                def convert_to_serializable(obj):
                    """Преобразует объект в сериализуемый формат."""
                    if hasattr(obj, 'isoformat'):
                        return obj.isoformat()
                    elif isinstance(obj, (datetime, date)):
                        return obj.isoformat()
                    else:
                        return obj

                modified_serializable = convert_to_serializable(modified)
                logger.info(f"💾 Сохраняем состояние: {table} = {modified_serializable}")

                self.state.set_state(key=table, value=modified_serializable)

                pkeys = [record['id'] for record in query_result]
                logger.info(f"🔑 Передаем {len(pkeys)} ключей в enricher: {pkeys[:5]}...")  # Покажем первые 5

                self.result_handler(
                    where_clause_table=table,
                    pkeys=pkeys,
                )
                logger.info(f"✅ FINISH: Обработка {table} завершена")
            else:
                logger.info(f"ℹ️ Нет новых записей в таблице {table}")

        except Exception as e:
            logger.error(f"❌ Ошибка в процессе извлечения для {table}: {e}")
            raise