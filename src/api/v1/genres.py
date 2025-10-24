from http import HTTPStatus

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel

from src.core import logger
from src.services.genres import GenresService, get_film_service

router = APIRouter()


class GenresResponse(BaseModel):
    id: str
    name: str

class GenresFullResponse(GenresResponse):
    film_titles: list[str]
    film_ids:list[str]
    films_count: int


@router.get('/{genres_id}', response_model=GenresFullResponse)
async def genres_details(genres_id: str, genres_service: GenresService = Depends(get_film_service)) -> GenresFullResponse:
    genres = await genres_service.get_by_id(genres_id)
    if not genres:

        raise HTTPException(status_code=HTTPStatus.NOT_FOUND, detail='genres not found')


    return GenresFullResponse(
        id=genres.id,
        name=genres.name,
        film_titles=genres.film_titles,
        film_ids=genres.film_ids,
        films_count=genres.films_count
    )


@router.get('/', response_model=list[GenresResponse])
async def genres_list(
        genres_service: GenresService = Depends(get_film_service)
) -> list[GenresResponse]:
    try:
        result = await genres_service.elastic.search(
            index='genres',
            body={
                "query": {"match_all": {}},
                "size": 100,
                "sort": [{"name": "asc"}]
            }
        )

        genres_list = []
        for hit in result['hits']['hits']:
            genres_data = hit['_source']
            genres_list.append(
                GenresResponse(
                    id=hit['_id'],
                    name=genres_data['name']
                )
            )

        return genres_list
    except Exception as e:
        logger.error(f"Error getting genres list: {e}")
        raise HTTPException(
            status_code=HTTPStatus.INTERNAL_SERVER_ERROR,
            detail='Internal server error'
        )