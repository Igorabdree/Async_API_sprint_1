import logging
from logging.config import dictConfig
from typing import Any

from lib.loggers import LOGGING
from database.backoff_connection import backoff
from elasticsearch import Elasticsearch, helpers
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
        """Конструктор класса ESLoader.

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
        state_data= self.state.get_state('data')
        if state_data:
            logger.debug('Найдены данные для продолжения обработки: %s записей', len(state_data))
            self.process(state_data)

    def convert_to_bulk_format(self, data):
        """Преобразовать данные в формат для bulk insert."""
        try:
            if not data.get('id'):
                logger.warning("⚠️ Документ без ID: %s", data)
                return None

            doc = {
                '_index': self.index,
                '_id': data['id'],
                '_source': {
                    'id': str(data['id']),
                    'imdb_rating': float(data['imdb_rating']) if data.get('imdb_rating') is not None else None,
                    'genres': list(data.get('genres', [])),
                    'title': str(data['title']) if data.get('title') else '',
                    'description': str(data.get('description', '')) if data.get('description') else None,
                    'directors_names': list(data.get('directors_names', [])),
                    'actors_names': list(data.get('actors_names', [])),
                    'writers_names': list(data.get('writers_names', [])),
                    'directors': list(data.get('directors', []) or []),
                    'actors': list(data.get('actors', []) or []),
                    'writers': list(data.get('writers', []) or []),
                }
            }

            self.simple_validation(doc['_source'])

            return doc

        except Exception as e:
            logger.exception(f"❌ Ошибка преобразования документа {data.get('id')}: {e}")
            return None

    def simple_validation(self, doc):
        """Простая валидация без удаления данных."""
        for field in ['directors', 'actors', 'writers', 'genres',
                      'directors_names', 'actors_names', 'writers_names']:
            if field not in doc or doc[field] is None:
                doc[field] = []
            elif not isinstance(doc[field], list):
                doc[field] = []

    def clean_es_data(self, data):
        """Очистить данные для Elasticsearch."""
        cleaned = data.copy()

        for field in ['directors', 'actors', 'writers']:
            if field in cleaned:
                if isinstance(cleaned[field], list):
                    cleaned[field] = [
                        person for person in cleaned[field]
                        if (isinstance(person, dict) and
                            person.get('id') is not None and
                            person.get('name') is not None)
                    ]
                else:
                    cleaned[field] = []

        return cleaned

    def remove_nulls(self, data):
        """Рекурсивно удалить null значения из данных."""
        if isinstance(data, dict):
            return {k: self.remove_nulls(v) for k, v in data.items() if v is not None}
        elif isinstance(data, list):
            return [self.remove_nulls(item) for item in data if item is not None]
        else:
            return data



    def process(self, data: dict) -> None:
        """Загрузить данные в Elasticsearch.

        Args:
            data: Данные для загрузки
        """
        self.state.get_state(key='data', default=data)
        self.bulk(list(map(self.convert_to_bulk_format, data)))
        self.state.set_state(key='data', value=None)

    @backoff()
    def create_index(self, index: str, index_schema: dict):
        """Создайте индекс, если индекс не существует.

        Args:
            index: Имя индекса.
            index_schema: Схема индекса.

        """
        if not self.client.indices.exists(index=index):
            self.client.indices.create(index=index, body=index_schema)

    def bulk(self, data):
        """Выполнить пакетную вставку данных."""
        try:
            success, errors = helpers.bulk(
                self.client,
                data,
                stats_only=False,
                raise_on_error=False  # Не прерывать при первой ошибке
            )

            if errors:
                logger.error(f"❌ Bulk insert errors: {len(errors)} documents failed")



            logger.info(f"✅ Успешно вставлено: {success}, ❌ Ошибок: {len(errors)}")
            return success, errors

        except Exception as exc:
            logger.exception("Bulk insert error")
            raise exc