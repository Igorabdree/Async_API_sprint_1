
import abc
import json
from typing import Any
import redis


class BaseStorage(abc.ABC):
    """Абстрактное хранилище состояния."""

    @abc.abstractmethod
    def save_state(self, state: dict[str, Any]) -> None:
        """Сохранить состояние в хранилище."""

    @abc.abstractmethod
    def retrieve_state(self) -> dict[str, Any]:
        """Получить состояние из хранилище."""


class JsonFileStorage(BaseStorage):
    """Реализация хранилища, использующего локальный файл."""

    def __init__(self, file_path: str) -> None:
        self.file_path = file_path

    def save_state(self, state: dict[str, Any]) -> None:
        """Сохранить состояние в хранилище."""
        with open(self.file_path, "w") as f:
            json.dump(state, f)

    def retrieve_state(self) -> dict[str, Any]:
        """Получить состояние из хранилища."""
        try:
            with open(self.file_path, "r") as f:
                return json.load(f)
        except (FileNotFoundError, json.JSONDecodeError):
            return {}


class RedisStorage(BaseStorage):
    """Реализация хранилища, использующего Redis."""
    def __init__(
            self,
            redis_settings: dict[str, Any],
            namespace: str = "movies_etl",
            service_name: str = "state_manager",
            state_key: str = "processing_state"
    ):
        """
        Args:
            redis_settings: Настройки подключения к Redis
            namespace: Пространство имен приложения
            service_name: Название сервиса
            state_key: Ключ для хранения состояния
        """
        self.redis_settings = redis_settings
        self._key = f"{namespace}:{service_name}:{state_key}"
        self._redis = None

    def _connect(self):
        """Установить соединение с Redis."""
        if self._redis is None:
            try:
                self._redis = redis.Redis(**self.redis_settings)
                self._redis.ping()  # Проверяем подключение
                print("✅ Redis подключен успешно")
            except Exception as e:
                print(f"❌ Ошибка подключения к Redis: {e}")
                raise

    def save_state(self, state: dict[str, Any]) -> None:
        """Сохранить состояние в Redis."""
        self._connect()


        serializable_state = self._make_serializable(state)
        serialized_state = json.dumps(serializable_state)
        self._redis.set(self._key, serialized_state)

    def retrieve_state(self) -> dict[str, Any]:
        """Получить состояние из Redis."""
        self._connect()

        try:
            serialized_state = self._redis.get(self._key)
            if serialized_state:
                return json.loads(serialized_state)
            return {}
        except (json.JSONDecodeError, Exception):
            return {}

    def _make_serializable(self, obj: Any) -> Any:
        """Преобразует объекты в сериализуемый формат."""
        if hasattr(obj, 'isoformat'):
            return obj.isoformat()
        elif isinstance(obj, dict):
            return {key: self._make_serializable(value) for key, value in obj.items()}
        elif isinstance(obj, (list, tuple)):
            return [self._make_serializable(item) for item in obj]
        else:
            return obj


class State:
    """Класс для работы с состояниями."""

    def __init__(self, storage: BaseStorage) -> None:
        self.storage = storage
        self.state = self.storage.retrieve_state()

    def set_state(self, key: str, value: Any) -> None:
        """Установить состояние для определённого ключа."""
        self.state[key] = value
        # Сразу сохраняем обновление в хранилище
        self.storage.save_state(self.state)

    def get_state(self, key: str, default: Any = None) -> Any:
        """Получить состояние по определённому ключу."""
        return self.state.get(key, default)

    def clear_state(self) -> None:
        """Очистить состояние."""
        self.state = {}
        self.storage.save_state(self.state)