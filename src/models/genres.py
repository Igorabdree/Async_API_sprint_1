from pydantic import BaseModel

class GenresResponse(BaseModel):
    id: str
    name: str

class GenresFullResponse(GenresResponse):
    film_titles: list[str]
    film_ids:list[str]
    films_count: int