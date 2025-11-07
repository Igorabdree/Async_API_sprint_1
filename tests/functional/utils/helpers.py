from typing import TypeVar, Annotated
from elasticsearch import AsyncElasticsearch

import pytest

T = TypeVar('T')
Fixture = Annotated[T, pytest.fixture]

class AbstractAsyncElasticsearch(AsyncElasticsearch):
    pass