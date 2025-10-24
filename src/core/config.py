from logging import config as logging_config
from pathlib import Path
from typing import ClassVar

from pydantic import Field
from src.core.logger import LOGGING
from pydantic_settings import BaseSettings, SettingsConfigDict

logging_config.dictConfig(LOGGING)

class Settings(BaseSettings):
    """
    Configuration settings for the application with validation.
    """

    model_config = SettingsConfigDict(
        env_file='.env',
        env_file_encoding='utf-8',
        case_sensitive=False,
        extra='ignore'
    )
    project_name: str = Field('movies', alias='PROJECT_NAME')

    redis_host: str = Field('127.0.0.1', alias='REDIS_HOST')
    redis_port: int = Field(6379, alias='REDIS_PORT')

    elastic_schema: str = Field('http://', alias='ELASTIC_SCHEMA')
    elastic_host: str = Field('127.0.0.1', alias='ELASTIC_HOST')
    elastic_port: int = Field(9200, alias='ELASTIC_PORT')
    BASE_DIR: ClassVar[Path] = Path(__file__).parent.parent.parent

    @property
    def elastic_url(self) -> str:
        """Полный URL для подключения к Elasticsearch"""
        return f"http://{self.elastic_host}:{self.elastic_port}"

settings = Settings()