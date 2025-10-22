"""Schemas to transform and validate data."""

from pydantic import BaseModel, Field, field_serializer
from typing import List, Optional

class Person(BaseModel):
    """Person data schema."""
    id: str
    name: str

class Movie(BaseModel):
    """Movie data schema."""
    id: str
    imdb_rating: Optional[float] = None
    genres: List[str] = Field(default_factory=list)
    title: str
    description: Optional[str] = None
    directors_names: List[str] = Field(default_factory=list)
    actors_names: List[str] = Field(default_factory=list)
    writers_names: List[str] = Field(default_factory=list)
    directors: List[Person] = Field(default_factory=list)
    actors: List[Person] = Field(default_factory=list)
    writers: List[Person] = Field(default_factory=list)

    @field_serializer('directors', 'actors', 'writers', 'genres',
                      'directors_names', 'actors_names', 'writers_names')
    def serialize_lists(self, value: List, _info):
        """Гарантируем что пустые списки сериализуются как [], а не null."""
        return value or []

    model_config = {
        'populate_by_name': True,
    }