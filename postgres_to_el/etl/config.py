"""Project settings."""
from lib import index_schema
from pydantic import Field
from pydantic_settings import BaseSettings


class PostgresSettings(BaseSettings):
    """Postgres connection settings."""
    host: str = Field('db', env='DB_HOST')
    # host: str = Field('127.0.0.1', env='DB_HOST')
    port: int = Field(5432, env='DB_PORT')
    dbname: str = Field('database', env='DB_NAME')
    # user: str = Field('test',env='DB_USER')
    user: str = Field('igor',env='DB_USER')
    password: str = Field('123',env='DB_PASSWORD')
    connect_timeout: int = 1


class ElasticsearchConnection(BaseSettings):
    """Elasticsearch connection settings."""
    # hosts: List[str] = Field(['http://localhost:9200'], env='ES_HOST')
    hosts: list[str] = Field(['http://elasticsearch:9200'], env='ES_HOST')
    request_timeout: int = Field(30, env='ES_TIMEOUT')
    max_retries: int = Field(3, env='ES_MAX_RETRIES')
    retry_on_timeout: bool = Field(True, env='ES_RETRY_ON_TIMEOUT')


class ElasticsearchSettings(BaseSettings):
    """Elasticsearch index settings."""
    connection: ElasticsearchConnection = ElasticsearchConnection()
    index: str = 'movies'
    request_timeout: int = Field(30, env='ES_TIMEOUT')  # было 'timeout'
    index_schema: dict = index_schema.movies
    verify_certs: bool = Field(False, env='ES_VERIFY_CERTS')
    ssl_show_warn: bool = Field(False, env='ES_SSL_SHOW_WARN')
    api_key: str | None = Field(None, env='ES_API_KEY')

class RedisSettings(BaseSettings):
    """Redis connection settings."""
    # host: str = Field('127.0.0.1', env='REDIS_HOST')
    host: str = Field("redis", env='REDIS_HOST')
    port: int = Field(6379, env='DEFAULT_REDIS_PORT')


class Cashe(BaseSettings):
    """Redis connection settings for every processor."""
    extractor: dict = {**RedisSettings().dict(), 'db': 1}
    enricher: dict = {**RedisSettings().dict(), 'db': 2}
    transformer: dict = {**RedisSettings().dict(), 'db': 3}
    loader: dict = {**RedisSettings().dict(), 'db': 4}


class Settings(BaseSettings):
    """Project settings."""

    postgres: PostgresSettings = PostgresSettings()
    es: ElasticsearchSettings = ElasticsearchSettings()
    cache: Cashe = Cashe()
    delay: int = 1
    page_size: int = 1000
    entities: set[str] = {'film_work', 'person', 'genre'}
    debug: str = Field('INFO', env='DEBUG')


settings = Settings()
