from pydantic import Field
from pydantic_settings import BaseSettings


class TestSettings(BaseSettings):
    es_host: str = Field('http://elasticsearch:9200', validation_alias='ELASTIC_HOST')
    es_index_movies: str = Field('movies_test', validation_alias='ES_INDEX')
    es_index_genres: str = Field('genres_test', validation_alias='ES_INDEX')
    es_id_field: str = Field('id', validation_alias='ES_ID_FIELD')
    es_index_mapping: dict = Field({}, validation_alias='ES_INDEX_MAPPING')

    redis_host: str = Field('redis', validation_alias='REDIS_HOST')
    redis_port: int = Field(6379, validation_alias='REDIS_PORT')

    service_url: str = Field('http://api:8000')

test_settings = TestSettings()