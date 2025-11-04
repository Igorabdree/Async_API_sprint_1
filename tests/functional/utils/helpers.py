from typing import TypeVar, Annotated

import pytest

T = TypeVar('T')
Fixture = Annotated[T, pytest.fixture]
