from typing import TypeVar,  Generator, Annotated

import pytest

T = TypeVar('T')
Fixture = Annotated[T, pytest.fixture]
