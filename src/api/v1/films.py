#ДЛЯ ДОКУМЕНТАЦИИ В СВАГЕР РАЗДЕЛ V1

from http import HTTPStatus
from typing import Optional, List

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel

from src.services.film import FilmService, get_film_service

router = APIRouter()


class FilmsResponse(BaseModel):
    id: str
    title: str
    imdb_rating: float

class FilmsDetailsResponse(FilmsResponse):
    description: Optional[str]
    genres: Optional[List]
    directors: Optional[List] = None
    actors: Optional[List] = None
    writers: Optional[List] = None

class FilmsListResponse(BaseModel):
    films: List[FilmsResponse]
    total: int
    page: int
    page_size: int
    total_pages: int

# Внедряем FilmService с помощью Depends(get_film_service)
@router.get('/{film_id}', response_model=FilmsDetailsResponse)
async def film_details(film_id: str, film_service: FilmService = Depends(get_film_service)) -> FilmsDetailsResponse:
    film = await film_service.get_by_id(film_id)
    if not film:
        # Если фильм не найден, отдаём 404 статус
        # Желательно пользоваться уже определёнными HTTP-статусами, которые содержат enum    # Такой код будет более поддерживаемым
        raise HTTPException(status_code=HTTPStatus.NOT_FOUND, detail='film not found')

    # Перекладываем данные из models.Film в Film
    # Обратите внимание, что у модели бизнес-логики есть поле description,
    # которое отсутствует в модели ответа API.
    # Если бы использовалась общая модель для бизнес-логики и формирования ответов API,
    # вы бы предоставляли клиентам данные, которые им не нужны
    # и, возможно, данные, которые опасно возвращать
    return FilmsDetailsResponse(
        id=film.id,
        title=film.title,
        imdb_rating=film.imdb_rating,
        description=film.description,
        genres=film.genres,
        actors=film.actors,
        writers=film.writers,
        directors=film.directors,
    )





@router.get('/', response_model=FilmsListResponse)
async def films_list(
        sort: str = Query(default="-imdb_rating", description="Сортировка: -imdb_rating (по убыванию) или imdb_rating (по возрастанию)"),
        page_size: int = Query(default=50, ge=1, le=100, description="Размер страницы"),
        page_number: int = Query(default=1, ge=1, description="Номер страницы"),
        film_service: FilmService = Depends(get_film_service)
) -> FilmsListResponse:
    """
    Получить список фильмов с пагинацией и сортировкой по рейтингу
    """
    films = await film_service.get_films_list(
        sort=sort,
        page_size=page_size,
        page_number=page_number
    )

    if not films:
        raise HTTPException(
            status_code=HTTPStatus.NOT_FOUND,
            detail='films not found'
        )

    return FilmsListResponse(
        films=[
            FilmsResponse(
                id=film.id,
                title=film.title,
                imdb_rating=film.imdb_rating
            ) for film in films
        ],
        total=len(films),
        page=page_number,
        page_size=page_size,
        total_pages=(len(films) + page_size - 1) // page_size
    )



@router.get('/search/', response_model=FilmsListResponse)
async def films_search(
        query: str = Query(description="Поиск по фильму"),
        page_size: int = Query(default=50, ge=1, le=100, description="Размер страницы"),
        page_number: int = Query(default=1, ge=1, description="Номер страницы"),
        film_service: FilmService = Depends(get_film_service)
) -> FilmsListResponse:
    """
    Получить список фильмов с пагинацией и сортировкой по рейтингу
    """
    films = await film_service.get_search_films(
        query=query,
        page_size=page_size,
        page_number=page_number
    )

    if not films:
        raise HTTPException(
            status_code=HTTPStatus.NOT_FOUND,
            detail='films not found'
        )

    return FilmsListResponse(
        films=[
            FilmsResponse(
                id=film.id,
                title=film.title,
                imdb_rating=film.imdb_rating
            ) for film in films
        ],
        total=len(films),
        page=page_number,
        page_size=page_size,
        total_pages=(len(films) + page_size - 1) // page_size
    )