
# Используем pydantic для упрощения работы при перегонке данных из json в объекты ДЛЯ ЭЛАСТИИКА
from pydantic import BaseModel


class FilmsResponseModel(BaseModel):
    id: str
    title: str
    imdb_rating: float

class FilmsDetailsResponseModel(FilmsResponseModel):
    """Модель ответа API для фильмов"""
    description: str | None
    genres: list | None
    directors: list | None = None
    actors: list | None = None
    writers: list | None = None