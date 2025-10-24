import logging
from logging.config import dictConfig
from typing import Any

from database.backoff_connection import backoff
from elasticsearch import Elasticsearch, helpers
from lib.loggers import LOGGING
from states import RedisStorage, State

dictConfig(LOGGING)
logger = logging.getLogger(__name__)


class Loader:

    def __init__(
            self,
            redis_settings: dict[str, Any],
            transport_options: dict[str, Any],
            index: str,
            index_schema: dict[str, Any] | None = None
    ) -> None:
        """Конструктор класса Loader для жанров.

        Args:
            redis_settings: Настройки подключения к Redis
            transport_options: Параметры подключения к Elasticsearch
            index: Название индекса Elasticsearch
            index_schema: Схема индекса. Если не None - индекс будет создан
        """

        self.client = Elasticsearch(**transport_options)
        self.storage = RedisStorage(redis_settings)
        self.state = State(self.storage)
        self.index = index

        if index_schema:
            self.create_index(index=index, index_schema=index_schema)

        self.proceed()

    def proceed(self) -> None:
        """Проверить состояние и продолжить работу, если есть данные в кэше."""
        state_data = self.state.get_state('data')
        if state_data:
            logger.debug('Найдены данные для продолжения обработки: %s записей', len(state_data))
            self.process(state_data)

    def convert_to_bulk_format(self, data: dict[str, Any]) -> dict[str, Any] | None:
        """Преобразовать данные жанра в формат для bulk insert."""
        try:
            if not data.get('id'):
                logger.warning("⚠️ Жанр без ID: %s", data)
                return None

            # Создаем документ для Elasticsearch
            doc = {
                '_index': self.index,
                '_id': data['id'],
                '_source': {
                    'id': str(data['id']),
                    'name': str(data.get('name', '')),
                    'film_titles': list(data.get('film_titles', [])),
                    'films_count': int(data.get('films_count', 0)),
                    'film_ids': list(data.get('film_ids', []))
                }
            }

            # Валидация данных
            self.validate_genre_document(doc['_source'])

            return doc

        except Exception as e:
            logger.exception(f"❌ Ошибка преобразования жанра {data.get('id')}: {e}")
            return None

    def validate_genre_document(self, doc: dict[str, Any]) -> None:
        """Валидация документа жанра."""
        # Проверяем обязательные поля
        if not doc.get('id'):
            raise ValueError("Genre document missing 'id' field")

        if not doc.get('name'):
            logger.warning("Genre %s missing name", doc.get('id'))

        # Обеспечиваем правильные типы
        if not isinstance(doc.get('films_count', 0), int):
            doc['films_count'] = 0

        if not isinstance(doc.get('film_ids', []), list):
            doc['film_ids'] = []

        # Очищаем пустые значения из film_ids
        doc['film_ids'] = [film_id for film_id in doc['film_ids'] if film_id]


    def process(self, data: list[dict[str, Any]]) -> None:
        """Загрузить данные жанров в Elasticsearch.

        Args:
            data: Список жанров для загрузки
        """
        if not data:
            logger.warning("No data to process")
            return

        # Сохраняем состояние
        self.state.set_state(key='data', value=data)

        # Преобразуем данные в bulk формат
        bulk_data = []
        for item in data:
            bulk_doc = self.convert_to_bulk_format(item)
            if bulk_doc:
                bulk_data.append(bulk_doc)

        if bulk_data:
            # Выполняем bulk вставку
            success_count, errors = self.bulk(bulk_data)
            logger.info("✅ Успешно загружено %s жанров, ❌ Ошибок: %s",
                        success_count, len(errors) if errors else 0)
        else:
            logger.warning("⚠️ Нет валидных данных для загрузки")

        # Очищаем состояние
        self.state.set_state(key='data', value=None)

    @backoff()
    def create_index(self, index: str, index_schema: dict[str, Any]) -> None:
        """Создать индекс, если он не существует.

        Args:
            index: Имя индекса.
            index_schema: Схема индекса.

        """
        if not self.client.indices.exists(index=index):
            try:
                self.client.indices.create(index=index, body=index_schema)
                logger.info("✅ Индекс %s создан успешно", index)
            except Exception as e:
                logger.error("❌ Ошибка создания индекса %s: %s", index, e)
                raise
        else:
            logger.debug("Индекс %s уже существует", index)

    @backoff()
    def bulk(self, data: list[dict[str, Any]]) -> tuple:
        """Выполнить пакетную вставку данных.

        Args:
            data: Данные для вставки

        Returns:
            tuple: (количество успешных операций, список ошибок)
        """
        try:
            success_count, errors = helpers.bulk(
                self.client,
                data,
                stats_only=False,
                raise_on_error=False
            )

            if errors:
                for error in errors[:5]:  # Логируем только первые 5 ошибок
                    logger.error("Bulk error: %s", error)
                logger.error("Всего ошибок при вставке: %s", len(errors))

            return success_count, errors

        except Exception as e:
            logger.exception("❌ Ошибка bulk вставки")
            raise e

    def cleanup(self) -> None:
        """Очистка ресурсов."""
        if self.client:
            self.client.close()