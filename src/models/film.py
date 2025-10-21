
# Используем pydantic для упрощения работы при перегонке данных из json в объекты ДЛЯ ЭЛАСТИИКА
from pydantic import BaseModel
from typing import Optional, List


class FilmsResponseModel(BaseModel):
    id: str
    title: str
    imdb_rating: float

class FilmsDetailsResponseModel(FilmsResponseModel):
    """Модель ответа API для фильмов"""
    description: Optional[str]
    genres: Optional[List]
    directors: Optional[List] = None
    actors: Optional[List] = None
    writers: Optional[List] = None
