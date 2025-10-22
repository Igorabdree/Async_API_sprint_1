"""Postgres function implementations."""

import logging
from logging.config import dictConfig

import psycopg2
import psycopg2.sql
# from lib.loggers import LOGGING
from lib.loggers import LOGGING
# from database.backoff_connection import backoff, backoff_reconnect

from database.backoff_connection import backoff, backoff_reconnect
from psycopg2.extras import RealDictCursor

dictConfig(LOGGING)
logger = logging.getLogger(__name__)


class PGConnection(object):
    """PG Connection class for backoff and execution PostgreSQL queiries.

    The class implements backoff and wraps up execution sql queries.

    Attributes:
        pg_settings : settings for PG connection.

    """
    def __init__(self, pg_settings: dict) -> None:
        """PGConnection class constructor.

        Args:
            pg_settings: settings for PG connection.

        """
        self.pg_settings = pg_settings
        self.connection = None
        self._safely_initialized = False

    @backoff()
    def _connect(self) -> None:
        """PG connection function with backoff wrapper."""
        logger.debug(
            'Connecting to the DB %s. Timeout %s',
            self.pg_settings['dbname'],
            self.pg_settings['connect_timeout'],
        )
        self.connection = psycopg2.connect(**self.pg_settings)
        self.connection.set_session(readonly=True, autocommit=True)

        logger.debug('Connected to the DB %s', self.pg_settings['dbname'])

    def __enter__(self):
        """Context manager entry."""
        self._connect()
        self._safely_initialized = True
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - close connection."""
        if self.connection:
            self.connection.close()
            logger.debug('Disconnected from DB')
        self._safely_initialized = False

    @backoff_reconnect()
    def retry_fetchall(self, sql: psycopg2.sql.Composed, **kwargs) -> RealDictCursor:
        """SQL query executor.

        Execute passed sql query and return results.

        Args:
            sql: SQL query.
            kwargs: keywordargs to pass into sql query.

        Returns:
            RealDictCursor: Records from database.

        """
        logger.debug('Try to execute sql %s. SQL PARAMS: %s', sql, kwargs)
        with self.connection.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(sql, (kwargs))
            return cursor.fetchall()
