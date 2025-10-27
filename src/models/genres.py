from pydantic import BaseModel

class GenresResponse(BaseModel):
    id: str
    name: str

class GenresFullResponse(GenresResponse):
    film_titles: list[str] | None
    film_ids:list[str] | None
    films_count: int | None