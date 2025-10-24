from pydantic import BaseModel, Field, field_serializer


class Person(BaseModel):
    """Схема данных для персоны."""
    id: str
    name: str

class Movie(BaseModel):

    """Схема данных фильма."""
    id: str
    imdb_rating: float | None = None
    genres: list[str] = Field(default_factory=list)
    title: str
    description: str | None = None
    directors_names: list[str] = Field(default_factory=list)
    actors_names: list[str] = Field(default_factory=list)
    writers_names: list[str] = Field(default_factory=list)
    directors: list[Person] = Field(default_factory=list)
    actors: list[Person] = Field(default_factory=list)
    writers: list[Person] = Field(default_factory=list)

    @field_serializer('directors', 'actors', 'writers', 'genres',
                      'directors_names', 'actors_names', 'writers_names')
    def serialize_lists(self, value: list, _info):
        """Гарантируем что пустые списки сериализуются как [], а не null."""
        return value or []

    model_config = {
        'populate_by_name': True,
    }