"""Genres ETL Service."""

import logging
from logging.config import dictConfig
from time import sleep

from lib.loggers import LOGGING
from database.pg_database import PGConnection
from enricher import Enricher
from extractor import Extractor
from loader import Loader
from transform import Transform
from config import settings

dictConfig(LOGGING)
logger = logging.getLogger(__name__)

pg = PGConnection(settings.postgres.model_dump())


if __name__ == '__main__':

    logger.info('Initializing')

    loader = Loader(
        redis_settings=settings.cache.loader,
        transport_options=settings.es.connection.model_dump(),
        index=settings.es.index,
        index_schema=settings.es.index_schema
    )

    transform = Transform(
        redis_settings=settings.cache.transformer,
        result_handler=loader.process,
    )

    enricher = Enricher(
        pg=pg,
        redis_settings=settings.cache.enricher,
        result_handler=transform.process,
        page_size=settings.page_size,
    )

    extractor = Extractor(
        pg=pg,
        redis_settings=settings.cache.extractor,
        result_handler=enricher.process,
    )


    logger.info('Started')
    while True:
        for entity in settings.entities:
            extractor.process(entity, page_size=settings.page_size)
            sleep(settings.delay)


